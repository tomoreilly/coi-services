#!/usr/bin/env python

'''
@package ion.services.dm.ingestion
@file ion/services/dm/ingestion/ingestion_worker.py
@author Swarbhanu Chatterjee
@brief IngestionWorker Class. An instance of this class acts as an ingestion worker. It is used to store incoming packets
to couchdb datastore and hdf datastore.
'''

from interface.objects import DataStream, StreamGranuleContainer
from pyon.datastore.datastore import DataStore
from pyon.public import log
from pyon.ion.transform import TransformDataProcess
from pyon.util.async import spawn

from pyon.datastore.couchdb.couchdb_datastore import sha1hex
from interface.objects import BlogPost, BlogComment, StreamPolicy
from pyon.core.exception import BadRequest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.event.event import StreamIngestionPolicyEventSubscriber




class IngestionWorker(TransformDataProcess):
    """
    Instances of this class acts as Ingestion Workers. They receive packets and send them to couchdb datastore or
    hdf storage according to the policy in the data stream or the default policy of the ingestion configuration
    """

    def on_start(self):
        super(IngestionWorker,self).on_start()
        #----------------------------------------------
        # Start up couch
        #----------------------------------------------

        self.couch_config = self.CFG.get('couch_storage')
        self.hdf_storage = self.CFG.get('hdf_storage')

        policy_dict = self.CFG.get('default_policy',{})
        self.default_policy = StreamPolicy()

        self.default_policy.archive_data = policy_dict.get('archive_data')
        self.default_policy.archive_metadata = policy_dict.get('archive_metadata')



        self.number_of_workers = self.CFG.get('number_of_workers')
        self.description = self.CFG.get('description')

        self.datastore_name = self.couch_config.get('datastore_name',None) or 'dm_datastore'
        try:
            self.datastore_profile = getattr(DataStore.DS_PROFILE, self.couch_config.get('datastore_profile','SCIDATA'))
        except AttributeError:
            log.exception('Invalid datastore profile passed to ingestion worker. Defaulting to SCIDATA')

            self.datastore_profile = DataStore.DS_PROFILE.SCIDATA
        log.debug('datastore_profile %s' % self.datastore_profile)
        self.db = self.container.datastore_manager.get_datastore(self.datastore_name, self.datastore_profile, self.CFG)

        self.resource_reg_client = ResourceRegistryServiceClient(node = self.container.node)


        self.stream_policies = {}
        # update the policy
        def receive_policy_event(event_msg, headers):
            log.warn('Got a message!!!!')
            self.stream_policies[event_msg.stream_id] = event_msg


        #Use the Exchang Point name (id?) as the origin for stream policy events
        XP = self.stream_subscriber_registrar.XP
        # @todo Find a better way to get the XP that the ingestion worker is subscribed too

        self.event_subscriber = StreamIngestionPolicyEventSubscriber(node = self.container.node, origin=XP, callback=receive_policy_event)
        self.gl = spawn(self.event_subscriber.listen)

        log.warn(str(self.db))

    def process(self, packet):
        """Process incoming data!!!!
        """

        # Get the policy for this stream
        policy = self.extract_policy_packet(packet)

        # Process the packet
        self.process_stream(packet, policy)


    def persist_immutable(self, obj):
        """
        This method is not functional yet - the doc object is python specific. The sha1 must be of a language independent form.
        """
        doc = self.db._ion_object_to_persistence_dict(obj)
        sha1 = sha1hex(doc)

        try:
            self.db.create_doc(doc, object_id=sha1)
        except BadRequest:
            # Deduplication in action!
            #@TODO why are we getting so many duplicate comments?
            log.exception('Failed to write packet!\n%s' % obj)

        # Do the id or revision have a purpose? do we need a return value?


    def process_stream(self, packet, policy):
        """
        Accepts a stream. Also accepts instruction (a policy). According to the received policy it processes the
        stream such as store in hfd_storage, couch_storage.
        @param: incoming_stream The incoming data stream of type stream.
        @param: policy The policy telling this method what to do with the incoming data stream.
        """

        # Ignoring is_replay attribute now that we have a policy implementation
        if isinstance(packet, StreamGranuleContainer):
            for key,value in packet.identifiables.iteritems():
                if isinstance(value, DataStream):
                    hdfstring = value
                    packet.identifiables[key]=''

            if policy.archive_metadata is True:
                self.persist_immutable(packet )

            if policy.archive_data is True:
                #@todo - save the hdf string somewhere..
                pass



        elif isinstance(packet, BlogPost) and not packet.is_replay:
            self.persist_immutable(packet )


        elif isinstance(packet, BlogComment) and not packet.is_replay:
            self.persist_immutable(packet)

        # Create any events for about the receipt of an update on this stream


    def on_stop(self):
        TransformDataProcess.on_stop(self)
        self.db.close()

    def on_quit(self):
        TransformDataProcess.on_quit(self)
        self.db.close()



    def extract_policy_packet(self, incoming_packet):
        """
        Extracts and returns the policy from the data stream
        """

        try:
            stream_id = incoming_packet.data_stream_id
        except AttributeError:
            log.info('Packet does not have a data_stream_id: using default policy')
            return self.default_policy


        policy = self.stream_policies(stream_id, None)

        if policy is None:
            policy = self.default_policy
            log.info('No policy found for stream id: %s - using default policy: %s' % (stream_id, policy))
        else:
            log.info('Got policy: %s for stream id: %s' % (policy, stream_id))


        # return the extracted instruction
        return policy