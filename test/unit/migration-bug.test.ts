
import { addRxPlugin } from 'rxdb';
import { RxDBMigrationSchemaPlugin } from 'rxdb/plugins/migration-schema';
import { createRxDatabase, RxCollection, removeRxDatabase } from 'rxdb';
import { RxJsonSchema } from 'rxdb';
import { replicateRxCollection } from 'rxdb/plugins/replication';
import { lastValueFrom, Subject, take, timeout } from 'rxjs';
import { getRxStorageMemory } from 'rxdb/plugins/storage-memory';
import { getRxStorageFilesystemNode } from 'rxdb-premium/plugins/storage-filesystem-node';
import path from 'node:path';
import { RxDBUpdatePlugin } from 'rxdb/plugins/update';

import assert from 'assert';
import AsyncTestUtil from 'async-test-util';
import config from './config.ts';

addRxPlugin(RxDBMigrationSchemaPlugin);
addRxPlugin(RxDBUpdatePlugin);

const schemaV1: RxJsonSchema<any> = {
  title: 'TestSchema',
  version: 0,
  type: 'object',
  primaryKey: 'id',
  properties: {
    id: { type: 'string', maxLength: 50 },
    foo: { type: 'string' },
  },
  required: ['id'],
};

const schemaV2: RxJsonSchema<any> = {
  title: 'TestSchema',
  version: 1,
  type: 'object',
  primaryKey: 'id',
  properties: {
    id: { type: 'string', maxLength: 50 },
    foo: { type: 'string' },
    bar: { type: 'string' },
  },
  required: ['id'],
};

const migrationStrategies = {
  1: (oldDoc) => oldDoc,
};

describe('migration-bug.test.ts', () => {

 if (!config.storage.hasMultiInstance) {
            return;
        }
    
  const dbName = randomToken(10);
  const identifier = 'items-pull';
  let collection: RxCollection;
  const pullStream$ = new Subject<any>();
  const storage = config.storage.getStorage()

  // function nextRev(rev: string): string {
  //   const n = parseInt(rev.split('-')[0], 10) + 1;
  //   return `${n}-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`;
  // }

  beforeAll(async () => {
    await removeRxDatabase(dbName, storage);
  });

  afterAll(async () => {
    pullStream$.complete();
    // await removeRxDatabase(dbName, storage);
  });

  it('should update document via replication stream BEFORE migration', async () => {
    const dbV1 = await createRxDatabase({
      name: dbName,
      storage: storage,
      multiInstance: false,
      closeDuplicates: true,
    });

    await dbV1.addCollections({
      items: { schema: schemaV1 },
    });

    collection = dbV1.items;
    await collection.insert({ id: 'a', foo: 'initial' });

    const replicationStateBefore = replicateRxCollection({
      collection,
      replicationIdentifier: identifier,
      live: true,
      pull: {
        handler: async () => ({ documents: [], checkpoint: null }),
        stream$: pullStream$.asObservable(),
        modifier: (d) => {
          console.log('modifier before', d);
          return d;
        },
      },
    });

    await replicationStateBefore.awaitInitialReplication();

   const sub1 = replicationStateBefore.received$.subscribe((doc) =>
      console.log('received BEFORE migration', doc),
    );
    // replicationStateBefore.error$.subscribe((doc) =>
    //   console.log('error BEFORE migration', doc),
    // );
    // replicationStateBefore.canceled$.subscribe((doc) =>
    //   console.log('canceled BEFORE migration', doc),
    // );
    // replicationStateBefore.active$.subscribe((doc) =>
    //   console.log('active BEFORE migration', doc),
    // );

    const preDoc = { id: 'a', foo: 'changed-before' };
    pullStream$.next({ documents: [preDoc], checkpoint: {} });

    await replicationStateBefore.awaitInSync();
   const emitted: any[] = [];
    const sub2 = collection
        .findOne({ selector: preDoc })
        .$.subscribe(doc => {
            emitted.push(doc);
        }))
    
   
    await AsyncTestUtil.waitUntil(() => emitted.length === 1);
    expect(docBefore!.toJSON()).toMatchObject(preDoc);

    await replicationStateBefore.cancel();
    sub1.unsubscribe();
    sub2.unsubscribe();
    await dbV1.close();
  });

  it('should update document via replication stream AFTER migration', async () => {
    const dbV2 = await createRxDatabase({
      name: dbName,
      storage: storage,
      multiInstance: false,
      closeDuplicates: true,
    });

    await dbV2.addCollections({
      items: {
        schema: schemaV2,
        migrationStrategies: migrationStrategies,
      },
    });

    collection = dbV2.items;

    // Pull-only Replikation nach der Migration
    const replicationStateAfter = replicateRxCollection({
      collection,
      replicationIdentifier: identifier,
      live: true,
      pull: {
        handler: async () => ({ documents: [], checkpoint: null }),
        stream$: pullStream$.asObservable(),
        modifier: (d) => {
          console.log('modifier after', d);
          return d;
        },
      },
    });

    await replicationStateAfter.awaitInitialReplication();

    replicationStateAfter.received$.subscribe((doc) =>
      console.log('received After migration', doc),
    );

    const postDoc = { id: 'a', foo: 'changed-after' };
    pullStream$.next({ documents: [postDoc], checkpoint: {} });

    await replicationStateAfter.awaitInSync();

    await lastValueFrom(
      collection
        .findOne({ selector: postDoc })
        .$.pipe(take(1), timeout({ first: 500 })),
    );

    const docAfter = await collection.findOne('a').exec();
    expect(docAfter!.toJSON()).toMatchObject(postDoc);

    await replicationStateAfter.cancel();

    await dbV2.close();
  });

  it('should update document via replication stream AFTER migration and local update', async () => {
    await removeRxDatabase(dbName, storage);
    const dbV1 = await createRxDatabase({
      name: dbName,
      storage: storage,
      multiInstance: false,
      closeDuplicates: true,
    });

    await dbV1.addCollections({
      items: {
        schema: schemaV1,
      },
    });
    collection = dbV1.items;
    await collection.insert({ id: 'a', foo: 'initial' });

    await dbV1.close();

    const dbV2 = await createRxDatabase({
      name: dbName,
      storage: storage,
      multiInstance: false,
      closeDuplicates: true,
    });

    await dbV2.addCollections({
      items: {
        schema: schemaV2,
        migrationStrategies: migrationStrategies,
      },
    });

    collection = dbV2.items;

    const docAfterPreRep = await collection.findOne('a').exec();
    await docAfterPreRep.update({
      $set: {
        foo: 'changed-preRep-afterMig',
      },
    });

    const docAfterPreRep_updaded = await collection.findOne('a').exec();
    expect(docAfterPreRep_updaded.toJSON()).toMatchObject({
      id: 'a',
      foo: 'changed-preRep-afterMig',
    });

    const replicationStateAfter = replicateRxCollection({
      collection,
      replicationIdentifier: identifier,
      live: true,
      pull: {
        handler: async () => ({ documents: [], checkpoint: null }),
        stream$: pullStream$.asObservable(),
        modifier: (d) => {
          console.log('modifier after', d);
          return d;
        },
      },
    });

    await replicationStateAfter.awaitInitialReplication();

    replicationStateAfter.received$.subscribe((doc) =>
      console.log('received After migration', doc),
    );

    const postDoc = { id: 'a', foo: 'changed-after' };
    pullStream$.next({ documents: [postDoc], checkpoint: {} });

    await replicationStateAfter.awaitInSync();

    await lastValueFrom(
      collection
        .findOne({ selector: postDoc })
        .$.pipe(take(1), timeout({ first: 500 })),
    );

    const docAfter = await collection.findOne('a').exec();
    expect(docAfter!.toJSON()).toMatchObject(postDoc);

    await replicationStateAfter.cancel();

    await dbV2.close();
  });
});
