import { Logger } from '@nestjs/common';
import {
  EventStoreDBClient,
  ChannelCredentialOptions,
  DNSClusterOptions,
  GossipClusterOptions,
  SingleNodeOptions,
  Credentials,
} from '@eventstore/db-client';
import { ConnectionEndpoint } from './event-store.module';

export class EventStore {
  connection: any;
  client: any;
  isConnected: boolean = false;
  retryAttempts: number;

  private logger: Logger = new Logger(this.constructor.name);

  constructor(
    private settings: ChannelCredentialOptions,
    private endpoint: ConnectionEndpoint,
    private credentials?: Credentials
  ) {
    this.retryAttempts = 0;
    this.connect();
  }

  connect() {
    return this.endpoint.hasOwnProperty('discover')
      ? this.connectDNSCluster()
      : this.endpoint.hasOwnProperty('endpoints')
      ? this.connectGossip()
      : this.connectSingleNode();
  }

  async connectGossip() {
    this.logger.log('Connecting as Gossip');
    this.connection = new EventStoreDBClient(
      this.endpoint as GossipClusterOptions,
      this.settings,
      this.credentials
    );
  }

  async connectDNSCluster() {
    this.logger.log('Connecting as DNSCluster');
    this.connection = new EventStoreDBClient(
      this.endpoint as DNSClusterOptions,
      this.settings,
      this.credentials
    );
  }

  async connectSingleNode() {
    this.logger.log('Connecting as SingleNode');
    this.connection = new EventStoreDBClient(
      this.endpoint as SingleNodeOptions,
      this.settings,
      this.credentials
    );
  }

  // async connect() {
  //   this.client = new EventStoreDBClient(
  //     // { endpoint: 'escluster.net:2113' },
  //     this.endpoint,
  //     this.settings,
  //     { username: 'admin', password: 'changeit' }
  //   );

  //   // this.client = await EventStoreDBClient.connectionString(
  //   //   'esdb+discover://escluster.net:2113'
  //   // );
  //   // console.log(this.client)
  //   // const event = jsonEvent<JSONEventType>({
  //   //   type: "TestEvent",
  //   //   data: {
  //   //     entityId: 123,
  //   //     importantData: "I wrote my first event!",
  //   //   },
  //   // });
  //   // console.log(await this.client.appendToStream("coucou", event));
  //   // console.log(await this.client.createPersistentSubscription('$ce-accountDto',
  //   // 'account',   persistentSubscriptionSettingsFromDefaults()    ))
  //   // console.log(await this.client.connectToPersistentSubscription(
  //   //   "$ce-accountDto",
  //   //   "account"
  //   // ))
  //   this.connection = this.client;
  //   // this.connection = createConnection(this.settings, this.endpoint);
  //   // this.connection.connect();
  //   // this.connection.on('connected', () => {
  //   //   this.logger.log('Connection to EventStore established!');
  //   //   this.retryAttempts = 0;
  //   //   this.isConnected = true;
  //   // });
  //   // this.connection.on('closed', (e) => {
  //   //   console.log(e)
  //   //   this.logger.error(`Connection to EventStore closed! reconnecting attempt(${this.retryAttempts})...`);
  //   //   this.retryAttempts += 1;
  //   //   this.isConnected = false;
  //   //   // this.connect();
  //   // });
  // }

  getConnection() {
    return this.connection;
  }

  close() {
    this.connection.close();
  }
}
