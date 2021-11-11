import { Global, Module, DynamicModule } from '@nestjs/common';
import { EventStore } from './event-store.class';
import {
  ChannelCredentialOptions,
  DNSClusterOptions,
  GossipClusterOptions,
  SingleNodeOptions,
} from '@eventstore/db-client';

export type ConnectionEndpoint =
  | DNSClusterOptions
  | GossipClusterOptions
  | SingleNodeOptions;

export interface EventStoreModuleOptions {
  endpoint: ConnectionEndpoint;
  settings: ChannelCredentialOptions;
}

export interface EventStoreModuleAsyncOptions {
  useFactory: (...args: any[]) => Promise<EventStoreModuleOptions> | any;
  inject?: any[];
}

@Global()
@Module({
  providers: [EventStore],
  exports: [EventStore],
})
export class EventStoreModule {
  static forRoot(options: EventStoreModuleOptions): DynamicModule {
    const { settings, endpoint } = options;
    return {
      module: EventStoreModule,
      providers: [
        {
          provide: EventStore,
          useFactory: () => {
            return new EventStore(settings, endpoint);
          },
        },
      ],
      exports: [EventStore],
    };
  }

  static forRootAsync(options: EventStoreModuleAsyncOptions): DynamicModule {
    return {
      module: EventStoreModule,
      providers: [
        {
          provide: EventStore,
          useFactory: async (...args) => {
            const { settings, endpoint, credentials } =
              await options.useFactory(...args);
            return new EventStore(settings, endpoint, credentials);
          },
          inject: options.inject,
        },
      ],
      exports: [EventStore],
    };
  }
}
