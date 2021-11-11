import { IEvent, Constructor } from '@nestjs/cqrs';
import { Subject } from 'rxjs';
import {
  StreamSubscription,
  PersistentSubscription,
  JSONEventType,
  jsonEvent,
  persistentSubscriptionSettingsFromDefaults,
  PersistentSubscriptionSettings,
  START,
} from '@eventstore/db-client';
import { Logger } from '@nestjs/common';
import { EventStore } from '../event-store.class';
import {
  EventStoreBusConfig,
  EventStoreSubscriptionType,
  EventStorePersistentSubscription as ESPersistentSubscription,
  EventStoreCatchupSubscription as ESCatchUpSubscription,
} from './event-bus.provider';

const ES_SUBSCRIPTIONS_MAX_RETRIES = 20;

export interface IEventConstructors {
  [key: string]: Constructor<IEvent>;
}

interface ExtendedCatchUpSubscription extends StreamSubscription {
  isLive: boolean | undefined;
}

interface ExtendedPersistentSubscription extends PersistentSubscription {
  isLive: boolean | undefined;
}

export class EventStoreBus {
  private retryAttempts: number = 0;
  private eventHandlers: IEventConstructors;
  private logger = new Logger('EventStoreBus');
  private catchupSubscriptions: ExtendedCatchUpSubscription[] = [];
  private catchupSubscriptionsCount: number;

  private persistentSubscriptions: ExtendedPersistentSubscription[] = [];
  private persistentSubscriptionsCount: number;

  constructor(
    private eventStore: EventStore,
    private subject$: Subject<IEvent>,
    config: EventStoreBusConfig
  ) {
    this.addEventHandlers(config.events);

    const catchupSubscriptions = config.subscriptions.filter((sub) => {
      return sub.type === EventStoreSubscriptionType.CatchUp;
    });

    const persistentSubscriptions = config.subscriptions.filter((sub) => {
      return sub.type === EventStoreSubscriptionType.Persistent;
    });

    this.subscribeToCatchUpSubscriptions(
      catchupSubscriptions as ESCatchUpSubscription[]
    );

    this.subscribeToPersistentSubscriptions(
      persistentSubscriptions as ESPersistentSubscription[]
    );
  }

  async subscribeToPersistentSubscriptions(
    subscriptions: ESPersistentSubscription[]
  ) {
    this.persistentSubscriptionsCount = subscriptions.length;

    await this.createMissingPersistentSubscriptions(subscriptions);

    this.persistentSubscriptions = await Promise.all(
      subscriptions.map(async (subscription) => {
        return await this.subscribeToPersistentSubscription(
          subscription.stream,
          subscription.persistentSubscriptionName
        );
      })
    );
  }

  async createMissingPersistentSubscriptions(
    subscriptions: ESPersistentSubscription[]
  ) {
    //TODO SEND SETTINGS (NOT YET?) SENT BY USER AS ARGUMENTS
    let settings: PersistentSubscriptionSettings =
      persistentSubscriptionSettingsFromDefaults();
    try {
      await Promise.all(
        subscriptions.map(async (subscription) => {
          return this.eventStore
            .getConnection()
            .createPersistentSubscription(
              subscription.stream,
              subscription.persistentSubscriptionName,
              settings
            )
            .then(() =>
              this.logger.log(
                `Created persistent subscription -
            ${subscription.persistentSubscriptionName}:${subscription.stream}`
              )
            )
            .catch(() => {});
        })
      );
    } catch (error) {
      this.logger.error(error);
    }
  }

  subscribeToCatchUpSubscriptions(subscriptions: ESCatchUpSubscription[]) {
    this.catchupSubscriptionsCount = subscriptions.length;
    this.catchupSubscriptions = subscriptions.map((subscription) => {
      return this.subscribeToCatchupSubscription(subscription.stream);
    });
  }

  get allCatchUpSubscriptionsLive(): boolean {
    const initialized =
      this.catchupSubscriptions.length === this.catchupSubscriptionsCount;
    return (
      initialized &&
      this.catchupSubscriptions.every((subscription) => {
        return !!subscription && subscription.isLive;
      })
    );
  }

  get allPersistentSubscriptionsLive(): boolean {
    const initialized =
      this.persistentSubscriptions.length === this.persistentSubscriptionsCount;
    return (
      initialized &&
      this.persistentSubscriptions.every((subscription) => {
        return !!subscription && subscription.isLive;
      })
    );
  }

  get isLive(): boolean {
    return (
      this.allCatchUpSubscriptionsLive && this.allPersistentSubscriptionsLive
    );
  }

  async publish(event: IEvent, stream?: string) {
    const evt = jsonEvent<JSONEventType>({
      type: event.constructor.name,
      data: JSON.parse(JSON.stringify(event)).data,
    });
    try {
      await this.eventStore.getConnection().appendToStream(stream, { ...evt });
    } catch (err) {
      this.logger.error(err.message, err.stack);
    }
  }

  async publishAll(events: IEvent[], stream?: string) {
    try {
      await this.eventStore.getConnection().appendToStream(
        stream,
        -2,
        (events || []).map((event: IEvent) =>
          jsonEvent<JSONEventType>({
            type: event.constructor.name,
            data: JSON.parse(JSON.stringify(event)).data,
          })
        )
      );
    } catch (err) {
      this.logger.error(err);
    }
  }

  subscribeToCatchupSubscription(stream: string): ExtendedCatchUpSubscription {
    this.logger.log(`Catching up and subscribing to stream ${stream}!`);
    try {
      var resolved = this.eventStore
        .getConnection()
        .subscribeToStreamFrom(stream)
        .on('data', (event) => this.onEvent(event, resolved))
        .on('close', () => this.onDropped(resolved, stream))
        .on('error', (error) => this.onError(error, stream))
        .on('confirmation', () =>
          this.onLiveProcessingStarted(resolved as ExtendedCatchUpSubscription)
        );
      return resolved;
    } catch (err) {
      this.logger.error(err.message, err.stack);
    }
  }

  async subscribeToPersistentSubscription(
    stream: string,
    subscriptionName: string
  ): Promise<ExtendedPersistentSubscription> {
    try {
      var resolved = await this.eventStore
        .getConnection()
        .connectToPersistentSubscription(stream, subscriptionName)
        .on('data', (event) => this.onEvent(event, resolved))
        .on('close', () => this.onDropped(resolved, stream, subscriptionName))
        .on('error', (error) => this.onError(error, stream, subscriptionName))
        .on('confirmation', () =>
          this.onPersistentProcessingStarted(stream, subscriptionName, resolved)
        );
      return resolved;
    } catch (err) {
      console.log('Error');
      this.logger.error(
        `[${stream}][${subscriptionName}] ${err.message}`,
        err.stack
      );
      this.reSubscribeToPersistentSubscription(stream, subscriptionName);
    }
  }

  async onEvent(payload: any, subscription: ExtendedPersistentSubscription) {
    const { event } = payload;

    //Acknowledge good reception
    if (event) await subscription.ack(payload);

    if (!event || !event.isJson) {
      this.logger.error('Received event that could not be resolved!');
      return;
    }
    const handler = this.eventHandlers[event.type];
    if (!handler) {
      this.logger.error('Received event that could not be handled!');
      return;
    }

    const data = Object.values({ data: event.data });
    this.subject$.next(new this.eventHandlers[event.type](...data));
  }

  onDropped(
    subscription: ExtendedPersistentSubscription,
    stream: string,
    subscriptionName?: string
  ) {
    subscription.isLive = false;
    this.retryAttempts = 0;
    this.logger.error(
      `[${stream}][${subscriptionName ?? 'CatchUp'}] Subscription dropped`
    );
    if (subscriptionName)
      this.reSubscribeToPersistentSubscription(stream, subscriptionName);
  }

  onError(error: Error, stream: string, subscriptionName?: string) {
    this.logger.error(
      `[${stream}][${subscriptionName ?? 'CatchUp'}] ${error.message}`
    );
    if (subscriptionName)
      this.reSubscribeToPersistentSubscription(stream, subscriptionName);
  }

  onPersistentProcessingStarted(
    stream: string,
    subscriptionName: string,
    subscription: ExtendedPersistentSubscription
  ) {
    subscription.isLive = true;
    this.logger.log(
      `Connection to persistent subscription ${subscriptionName} on stream ${stream} established!`
    );
  }

  reSubscribeToPersistentSubscription(
    stream: string,
    subscriptionName: string
  ) {
    if (
      this.retryAttempts <
      ES_SUBSCRIPTIONS_MAX_RETRIES * this.persistentSubscriptionsCount
    ) {
      this.logger.warn(
        `connecting to subscription ${subscriptionName} ${stream}. Retrying...`
      );
      this.retryAttempts += 1;
      setTimeout(
        (stream, subscriptionName) =>
          this.subscribeToPersistentSubscription(stream, subscriptionName),
        3000,
        stream,
        subscriptionName
      );
    }
  }

  onLiveProcessingStarted(subscription: ExtendedCatchUpSubscription) {
    subscription.isLive = true;
    this.logger.log('Live processing of EventStore events started!');
  }

  addEventHandlers(eventHandlers: IEventConstructors) {
    this.eventHandlers = { ...this.eventHandlers, ...eventHandlers };
  }
}
