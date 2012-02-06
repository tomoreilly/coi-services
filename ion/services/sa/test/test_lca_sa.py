#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, IonObject
#from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.imarine_facility_management_service import MarineFacilityManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS # , PRED
from nose.plugins.attrib import attr
import unittest

from ion.services.sa.test.helpers import any_old

class FakeProcess(LocalContextMixin):
    name = ''


# some stuff for logging info to the console
import sys
log = DotDict()
printout = sys.stderr.write
printout = lambda x: None

log.debug = lambda x: printout("DEBUG: %s\n" % x)
log.info = lambda x: printout("INFO: %s\n" % x)
log.warn = lambda x: printout("WARNING: %s\n" % x)



@attr('INT', group='sa')
class TestLCASA(IonIntegrationTestCase):
    """
    LCA integration tests

    tests that start with test_jg_slide reference slides found here:
    https://confluence.oceanobservatories.org/download/attachments/33753448/LCA_Demo_Swimlanes_CI_2012-01-20_ver_0-06.pdf
    """

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2sa.yml')

        # Now create client to DataProductManagementService
        self.client = DotDict()
        self.client.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.client.DPMS = DataProductManagementServiceClient(node=self.container.node)
        self.client.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.client.MFMS = MarineFacilityManagementServiceClient(node=self.container.node)

        # number of ion objects per type
        self.ionobj_count = {}


    def test_just_the_setup(self):
        return

    #@unittest.skip('temporarily')
    def test_lca_step_1(self):
        log.info("LCA steps 1.3, 1.4, 1.5, 1.6, 1.7: FCRUF marine facility")
        self.generic_fcruf_script(RT.MarineFacility, "marine_facility", self.client.MFMS, True)

    #@unittest.skip('temporarily')
    def test_lca_step_3(self):
        log.info("LCA steps 3.1, 3.2, 3.3, 3.4: FCRF site")
        self.generic_fcruf_script(RT.Site, "site", self.client.MFMS, True)


    #@unittest.skip('temporarily')
    def test_lca_step_4(self):
        c = self.client

        log.info("setting up requirements for LCA step 4: a site")
        site_id = self.generic_fcruf_script(RT.Site, "site", self.client.MFMS, True)

        log.info("LCA step 4.1, 4.2: FCU platform model")
        platform_model_id = self.generic_fcruf_script(RT.PlatformModel, 
                                                     "platform_model", 
                                                     self.client.IMS, 
                                                     True)

        log.info("LCA step 4.3, 4.4: CF logical platform")
        logical_platform_id = self.generic_fcruf_script(RT.LogicalPlatform, 
                                                    "logical_platform", 
                                                    self.client.MFMS, 
                                                    True)
        
        log.info("LCA step 4.5: C platform device")
        platform_device_id = self.generic_fcruf_script(RT.PlatformDevice, 
                                                    "platform_device", 
                                                    self.client.IMS, 
                                                    False)

        log.info("LCA step 4.6: Assign logical platform to site")
        c.MFMS.assign_logical_platform_to_site(logical_platform_id, site_id)

        #TODO: LCA script seems to be missing "assign_logical_platform_to_platform_device"


        # code to delete what we created is in generic_d_script
        self.generic_d_script(site_id, "site", self.client.MFMS)
        self.generic_d_script(platform_model_id, "platform_model", self.client.IMS)
        self.generic_d_script(logical_platform_id, "logical_platform", self.client.MFMS)
        self.generic_d_script(platform_device_id, "platform_device", self.client.IMS)


    def test_lca_step5(self):
        c = self.client

        log.info("setting up requirements for LCA step 5: site + logical platform")
        site_id = self.generic_fcruf_script(RT.Site, "site", self.client.MFMS, True)
        logical_platform_id = self.generic_fcruf_script(RT.LogicalPlatform, 
                                                    "logical_platform", 
                                                    self.client.MFMS, 
                                                    True)

        log.info("LCA step 5.1, 5.2: FCU instrument model")
        instrument_model_id = self.generic_fcruf_script(RT.InstrumentModel, 
                                                       "instrument_model", 
                                                       self.client.IMS, 
                                                       True)

        log.info("LCA step 5.3: CU logical instrument")
        logical_instrument_id = self.generic_fcruf_script(RT.LogicalInstrument, 
                                                    "logical_instrument", 
                                                    self.client.MFMS, 
                                                    True)

        log.info("Assigning logical instrument to logical platform")
        c.MFMS.assign_logical_instrument_to_logical_platform(logical_instrument_id, logical_platform_id)



        log.info("LCA step 5.4: list logical instrument by platform")
        #TODO

        log.info("LCA step 5.5: list instruments by observatory")
        #TODO

        log.info("LCA step 5.6, 5.7, 5.9: CRU instrument_device")
        instrument_device_id = self.generic_fcruf_script(RT.InstrumentDevice, 
                                                    "instrument_device", 
                                                    self.client.IMS, 
                                                    False)

        log.info("LCA step 5.8: instrument device policy?")
        #TODO
        
        log.info("LCA step 5.10a: find data products by instrument device")
        
        #find data products
        products = self.client.IMS.find_data_product_by_instrument_device(instrument_device_id)
        print products

        log.info("LCA step 5.10b: find data products by platform")
        log.info("LCA step 5.10c: find data products by site")
        log.info("LCA step 5.10d: find data products by marine facility")
        #TODO



        #delete what we created
        self.generic_d_script(instrument_device_id, "instrument_device", self.client.IMS)
        self.generic_d_script(instrument_model_id, "instrument_model", self.client.IMS)
        self.generic_d_script(logical_instrument_id, "logical_instrument", self.client.MFMS)
        self.generic_d_script(logical_platform_id, "logical_platform", self.client.MFMS)
        self.generic_d_script(site_id, "site", self.client.MFMS)


    #@unittest.skip('temporarily')
    def test_lca_step_6(self):

        log.info("setting up requirements for LCA step 6: instrument model")
        instrument_model_id = self.generic_fcruf_script(RT.InstrumentModel, 
                                                       "instrument_model", 
                                                       self.client.IMS, 
                                                       True)


        log.info("LCA step 6.1, 6.2: FCU instrument agent")
        instrument_agent_id = self.generic_fcruf_script(RT.InstrumentAgent, 
                                                       "instrument_agent", 
                                                       self.client.IMS, 
                                                       True)        
        
        log.info("LCA step 6.3: associate instrument model to instrument agent")
        #TODO

        log.info("LCA step 6.4: find instrument model by instrument agent")
        #TODO

        #delete what we created
        self.generic_d_script(instrument_agent_id, "instrument_agent", self.client.IMS)
        self.generic_d_script(instrument_model_id, "instrument_model", self.client.IMS)








    def generic_d_script(self, resource_id, resource_label, owner_service):
        """
        delete a resource and check that it was properly deleted

        @param resource_id id to be deleted
        @param resource_label something like platform_model
        @param owner_service service client instance
        """

        del_op = getattr(owner_service, "delete_%s" % resource_label)
        
        del_op(resource_id)

        # try again to make sure that we get NotFound
        self.assertRaises(NotFound, del_op, resource_id)


    def generic_fcruf_script(self, resource_iontype, resource_label, owner_service, is_simple):
        """
        run through find, create, read, update, and find ops on a basic resource

        NO DELETE in here.

        @param resource_iontype something like RT.BlahBlar
        @param resource_label something like platform_model
        @param owner_service a service client instance
        @param is_simple whether to check for AVAILABLE LCS on create
        """

        # this section is just to make the LCA integration script easier to write.
        #
        # each resource type gets put through (essentially) the same steps.
        #
        # so, we just set up a generic service-esque object.
        # (basically just a nice package of shortcuts):
        #  create a fake service object and populate it with the methods we need

        some_service = DotDict()

        def make_plural(noun):
            if "y" == noun[-1]:
                return noun[:-1] + "ies"
            else:
                return noun + "s"
            
        
        def fill(svc, method, plural=False):
            """
            make a "shortcut service" for testing crud ops.  
            @param svc a dotdict 
            @param method the method name to add
            @param plural whether to make the resource label plural
            """

            reallabel = resource_label
            realmethod = "%s_widget" % method
            if plural:
                reallabel = make_plural(reallabel)
                realmethod = realmethod + "s"
                
            setattr(svc, realmethod,  
                    getattr(owner_service, "%s_%s" % (method, reallabel)))


        
        fill(some_service, "create")
        fill(some_service, "read")
        fill(some_service, "update")
        fill(some_service, "delete")
        fill(some_service, "find", True)



        #UX team: generic script for LCA resource operations begins here.
        # some_service will be replaced with whatever service you're calling
        # widget will be replaced with whatever resource you're working with
        # resource_label will be data_product or logical_instrument


        resource_labels = make_plural(resource_label)

        log.info("Finding %s" % resource_labels)
        num_objs = len(some_service.find_widgets())
        log.info("I found %d %s" % (num_objs, resource_labels))

        log.info("Creating a %s" % resource_label)
        generic_obj = any_old(resource_iontype)
        generic_id = some_service.create_widget(generic_obj)

        log.info("Reading %s #%s" % (resource_label, generic_id))
        generic_ret = some_service.read_widget(generic_id)

        log.info("Verifying equality of stored and retrieved object")
        self.assertEqual(generic_obj.name, generic_ret.name)
        self.assertEqual(generic_obj.description, generic_ret.description)


        #"simple" resources go available immediately upon creation, so check:
        if is_simple:
            log.info("Verifying that resource went AVAILABLE on creation")
            self.assertEqual(generic_ret.lcstate, LCS.AVAILABLE)

        log.info("Updating %s #%s" % (resource_label, generic_id))
        generic_newname = "%s updated" % generic_ret.name
        generic_ret.name = generic_newname
        some_service.update_widget(generic_ret)

        log.info("Reading platform model #%s to verify update" % generic_id)
        generic_ret = some_service.read_widget(generic_id)

        self.assertEqual(generic_newname, generic_ret.name)
        self.assertEqual(generic_obj.description, generic_ret.description)

        log.info("Finding platform models... checking that there's a new one")
        num_objs2 = len(some_service.find_widgets())

        self.assertTrue(num_objs2 > num_objs)

        return generic_id