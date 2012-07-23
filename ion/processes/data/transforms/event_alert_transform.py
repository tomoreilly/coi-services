#!/usr/bin/env python

'''
@brief The EventAlertTransform listens to events and publishes alert messages when the events
        satisfy a condition. Its uses an algorithm to check the latter
@author Swarbhanu Chatterjee
'''
from pyon.ion.transforma import TransformEventListener, TransformEventPublisher, TransformAlgorithm
from pyon.util.log import log
from interface.objects import ProcessDefinition
from ion.services.dm.utility.query_language import QueryLanguage
from pyon.core.exception import BadRequest
from pyon.event.event import EventPublisher

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
import operator

class EventAlertTransform(TransformEventListener):

    def on_start(self):
        log.warn('TransformDataProcess.on_start()')

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # config to the listener (event types etc and the algorithm)
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        algorithm = self.CFG.get_safe('process.algorithm', None)
        event_type = self.CFG.get_safe('process.event_type', '')
        event_origin = self.CFG.get_safe('process.event_origin', '')
        event_origin_type = self.CFG.get_safe('process.event_origin_type', '')
        event_subtype = self.CFG.get_safe('process.event_subtype', '')

        #-------------------------------------------------------------------------------------
        # Create a transform event listener
        #-------------------------------------------------------------------------------------


        # The configuration for the listener
        configuration_listener = {
                                    'process':{
                                                'algorithm': algorithm,
                                                'event_type': event_type,
                                                'event_origin': event_origin,
                                                'event_origin_type': event_origin_type,
                                                'event_subtype': event_subtype,
                                                'callback' : self.publish
                                        }
                                }
        # Create the process
        pid = EventAlertTransform.create_process(   name= 'transform_event_listener',
                                                    module='ion.processes.data.transforms.transform',
                                                    class_name='TransformEventListener',
                                                    configuration= configuration_listener)

        #-------------------------------------------------------------------------------------
        # Create the publisher that will publish the Alert message
        #-------------------------------------------------------------------------------------

        self.event_publisher = EventPublisher(event_type=event_type)

    def publish(self):

        self.event_publisher.publish_event( event_type= "DeviceEvent",
                                            origin="EventAlertTransform",
                                            description= "An alert event being published.")

    @staticmethod
    def create_process(name= '', module = '', class_name = '', configuration = None):
        '''
        A helper method to create a process
        '''

        process_dispatcher = ProcessDispatcherServiceClient()

        producer_definition = ProcessDefinition(name=name)
        producer_definition.executable = {
            'module':module,
            'class': class_name
        }

        procdef_id = process_dispatcher.create_process_definition(process_definition=producer_definition)
        pid = process_dispatcher.schedule_process(process_definition_id= procdef_id, configuration=configuration)

        return pid

class Operation(object):
    '''
    Apply a user provided operator on a set of fields and return the result.
    This is meant to be an object that can be fed to an Algorithm object, which in turn will
    check whether the result is consistent with its own query dict obtained by parsing a query statement.
    '''

    operators = {   "+"  :  operator.add,
                    "-"  :  operator.sub,
                    "*"  :  operator.mul,
                    "/"  :  operator.div
                }


    def __init__(self, _operator = '+', _operator_list = None):

        #-------------------------------------------------------------------------------------
        # A simple operator
        #-------------------------------------------------------------------------------------
        self._operator = _operator

        #-------------------------------------------------------------------------------------
        # instead of a simple single operator, one could provide a list of operators,
        # Ex: ['+', '-', '+', '*'] ==> a + b - c + d * e
        #-------------------------------------------------------------------------------------

        self._operator_list = _operator_list

    def define_subtraction_constant(self, const):
        '''
        If we want to do a successive subtraction operation, it may be relevant to have a starting point unequal to 0.
        For example, subtract all field values from, let's say, 100.
        '''
        self.const = const

    def _initialize_the_result(self):

        # apply the operator on the fields
        if self._operator_list:
            result = 0
        elif self._operator == '+':
            result = 0
        elif  self._operator == '-':
            if self.const:
                result = self.const
            else: result = 0
        elif self._operator == '*' or '/':
            result = 1
        else:
            raise NotImplementedError("Unimplemented operator: %s" % self._operator)

        return result

    def execute(self, fields = None):

        #todo this method is most useful for additions

        #-------------------------------------------------------------------------------------
        # Initialize the result
        #-------------------------------------------------------------------------------------

        result = self._initialize_the_result()

        #-------------------------------------------------------------------------------------
        # Apply the operation
        #-------------------------------------------------------------------------------------

        if not self._operator_list: # if operator list is not provided, apply the simple SINGLE operator
            for field in fields:
                # get the Python operator
                self.operator =  Operation.operators[self._operator]
                result = self.operator(result, field)
        else: # apply operator list
            count = 0
            for field in fields:
                operator = Operation.operators[self._operator_list][count]
                result = operator(result, field)

        return result


class AlgorithmA(TransformAlgorithm):
    '''
    This is meant to be flexible, accept a query statement and return True/False.

    To use this object:

        algorithm = AlgorithmA( statement = "search 'result' is '5' from 'dummy_index' and SEARCH 'result' VALUES FROM 10 TO 20 FROM 'dummy_index",
                                fields = [1,20,3,10],
                                _operator = '+',
                                _operator_list = ['+','-','*'])

        algorithm.execute()

        This going to check for (1 + 20 -3 * 10 is equal to 5 OR 1 + 20 -3 * 10 has a value between 10 and 20)



    '''

    def __init__(self, statement = '', fields = None, _operator = '+', _operator_list = None):
        self.ql = QueryLanguage()
        self.statement = statement
        self.fields = fields

        # the number of operations have to be one less than the number of fields
        if _operator_list and len(_operator_list) != len(fields) - 1:
            raise AssertionError("An operator list has been provided but does not correspond correctly with number of " \
                                 "field values to operate on" )


        self.operation = Operation(_operator= _operator, _operator_list = _operator_list)

    def execute(self):

        #-------------------------------------------------------------------------------------
        # Construct the query dictionary after parsing the string statement
        #-------------------------------------------------------------------------------------

        query_dict = self.ql.parse(self.statement)

        #-------------------------------------------------------------------------------------
        # Execute the operation on the fields and get the result out
        #-------------------------------------------------------------------------------------

        result = self.operation.execute(self.fields)

        #-------------------------------------------------------------------------------------
        # Check if the result satisfies the query dictionary
        #-------------------------------------------------------------------------------------

        evaluation = self.evaluate_condition(result, query_dict)

        return evaluation

    def evaluate_condition(self, result = None, query_dict = None):
        '''
        If result matches the query dict return True, else return False
        '''

        main_query = query_dict['query']
        or_queries= query_dict['or']
        and_queries = query_dict['and']

        #-------------------------------------------------------------------------------------
        # if any of the queries in the list of 'or queries' gives a match, publish an event
        #-------------------------------------------------------------------------------------
        if or_queries:
            for or_query in or_queries:
                if self.match(result, or_query):
                    return True

        #-------------------------------------------------------------------------------------
        # if an 'and query' or a list of 'and queries' is provided, return if the match returns false for any one of them
        #-------------------------------------------------------------------------------------
        if and_queries:
            for and_query in and_queries:
                if not self.match(result, and_query):
                    return False

        #-------------------------------------------------------------------------------------
        # The main query
        #-------------------------------------------------------------------------------------
        return self.match(result, main_query)


    def match(self, result = None, query = None):
        '''
        Checks whether it is an "equals" matching or a "range" matching
        '''

        if QueryLanguage.query_is_term_search(query):
            # This is a term search - always a string
            if str(result) == query['value']:
                return True

        elif QueryLanguage.query_is_range_search(query):
            # always a numeric value - float or int
            if (result >=  query['range']['from']) and (result <= query['range']['to']):
                return True
            else:
                return False

            pass
        else:
            raise BadRequest("Missing parameters value and range for query: %s" % query)


#    ss = "search 'result' is '5' from 'dummy_index' and SEARCH 'result' VALUES FROM 10 TO 20 FROM 'dummy_index' "
#
#    query_dict = {       'and': [{'field': 'result',
#                                          'index': 'dummy_index',
#                                          'range': {'from': 10.0, 'to': 20.0}}],
#                                 'or': [],
#                                 'query': {'field': 'result', 'index': 'dummy_index', 'value': '5'}}






