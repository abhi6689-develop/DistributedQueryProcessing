from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function


import csv
import logging
from enum import Enum
from parser import tuple2st
from re import L, S
from time import time
from typing import List, Tuple
import uuid
import argparse # For command line arguments
import time
import ray
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


# Generates unique operator IDs
def _generate_uuid():
    return uuid.uuid4()

# Partition strategy enum
class PartitionStrategy(Enum):
    RR = "Round_Robin"
    HASH = "Hash_Based"

# Custom tuple class with optional metadata
class ATuple:
    """Custom tuple.

    Attributes:
        tuple (Tuple): The actual tuple.
        metadata (string): The tuple metadata (e.g. provenance annotations).
        operator (Operator): A handle to the operator that produced the tuple.
    """
    def __init__(self, tuple, metadata=None, operator=None):
        self.tuple = tuple
        self.metadata = metadata
        self.operator = operator

    # Returns the lineage of self
    def lineage(self) -> List[ATuple]:
        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(self, att_index) -> List[Tuple]:
        pass

    # Returns the How-provenance of self
    def how(self) -> str:
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs(self) -> List[Tuple]:
        pass

# Data operator
class Operator:
    """Data operator (parent class).

    Attributes:
        id (string): Unique operator ID.
        name (string): Operator name.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    def __init__(self,
                 id=None,
                 name=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        self.id = _generate_uuid() if id is None else id
        self.name = "Undefined" if name is None else name
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov
        self.pull = pull
        self.partition_strategy = partition_strategy
        logger.debug("Created {} operator with id {}".format(self.name,
                                                             self.id))

    def get_next(self) -> List[ATuple]:
        logger.error("Method not implemented!")

    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        logger.error("Lineage method not implemented!")

    def where(self, att_index: int, tuples: List[ATuple]) -> List[List[Tuple]]:
        logger.error("Where-provenance method not implemented!")

    def apply(self, tuples: List[ATuple]) -> bool:
        logger.error("Apply method is not implemented!")

# Scan operator
@ray.remote
class Scan(Operator):
    """Scan operator.

    Attributes:
        filepath (string): The path to the input file.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        filter (function): An optional user-defined filter.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes scan operator
    def __init__(self,
                 filepath,
                 outputs : List[Operator],
                 side,
                 filter=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 chunksize = 100000,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        Operator.__init__(self, name="Scan",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        self.filepath = filepath
        self.outputs = outputs
        self.filter = filter
        self.chunksize = chunksize
        self.left_tuples = [] #will store the smaller table in memory
        self.last_row = 0
        self.exhausted = False #will be set to true when the scan is exhausted
        self.side = side
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logging.info(msg)

    # Returns next batch of tuples in given file (or None if file exhausted)

    
    def get_exhausted(self):
        return self.exhausted
    
    def set_exhausted(self):
        self.exhausted = True

    def get_next(self):

        """
        Summary: Returns next batch of tuples in given file (or None if file exhausted)

        inputs: None

        Returns:
            List[ATuple]: A list of ATuples.
        """        
        self.log("Scan operator {} is getting next batch of tuples".format(self.id))
        batch = []
        row_num = 0 

        with open(self.filepath, 'r') as f:
            reader = csv.reader(f, delimiter=' ')
            header = ATuple(tuple(next(reader)[1:]))
            if self.last_row != 0:
                for i in range(self.last_row):
                    next(reader)
            for row in reader:
                row_num += 1
                row = ATuple(tuple(row))
                batch.append(row)
                if row_num % self.chunksize == 0:
                    self.last_row += self.chunksize
                    batch.append(header)
                    batch = list(map(lambda x: x.tuple, batch))
                    return batch
            if len(batch) > 0:
                batch.append(header)
                batch = list(map(lambda x: x.tuple, batch))
                self.set_exhausted()
                return batch
                            
                            
    def get_last_row(self):
        return self.last_row
    
    def set_last_row(self, row):
        self.last_row = row

    # Starts the process of reading tuples (only for push-based evaluation)
    def start(self):
        row_num = 0 
        self.log("Scan operator {} is starting".format(self.id))
        with open(self.filepath, 'r') as f:
            batch = []
            reader = csv.reader(f, delimiter=' ')
            header = ATuple(tuple(next(reader)[1:]))
            # if self.get_last_row() != 0:
            #     for i in range(self.get_last_row()):
            #         next(reader)
            for row in reader:
                row_num += 1
                row = ATuple(tuple(row))
                batch.append(row)
                if row_num % self.chunksize == 0:
                    # self.set_last_row(self.last_row + self.chunksize)
                    batch.append(header)
                    # batch = list(map(lambda x: x.tuple, batch))
                    self.outputs[0].set_side.remote(self.side)
                    self.outputs[0].apply.remote(tuples = batch)
                    batch = []
            if len(batch) > 0:
                batch.append(header)
                # batch = list(map(lambda x: x.tuple, batch))
                self.outputs[0].set_side.remote(self.side)
                self.outputs[0].apply.remote(tuples = batch)
            
            

# Equi-join operator
@ray.remote
class Join(Operator):
    """Equi-join operator.

    Attributes:
        left_inputs (List): A list of handles to the instances of the operator
        that produces the left input.
        right_inputs (List):A list of handles to the instances of the operator
        that produces the right input.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        left_join_attribute (int): The index of the left join attribute.
        right_join_attribute (int): The index of the right join attribute.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes join operator
    def __init__(self,
                 left_inputs : List[Operator],
                 right_inputs : List[Operator],
                 outputs : List[Operator],
                 side,
                 left_join_attribute,
                 right_join_attribute,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        Operator.__init__(self, name="Join",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        self.left_inputs = left_inputs
        self.right_inputs = right_inputs
        self.outputs = outputs
        self.side = side
        self.left_join_attribute = left_join_attribute
        self.right_join_attribute = right_join_attribute
        self.joined_tuples = [] #will store the joined tuples
        self.left_dict = {} #will store the left table in memory
        self.right_dict = {} #will store the unmatched tuples from the right table in memory
        self.right_attributes = []
        self.left_attributes = []
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logging.info(msg)



    # Returns next batch of joined tuples (or None if done)
    def get_next(self):
        self.log("Join operator {} is getting next batch of tuples".format(self.id))
        left_dict = {}
        while ray.get(self.left_inputs[0].get_exhausted.remote()) == False:
            for i in ray.get(self.left_inputs[1].get_next.remote()):
                left_attributes = ray.get(self.left_inputs[1].get_headers.remote())
                left_attribute_index = left_attributes.index(self.left_join_attribute)
                left_dict[i[left_attribute_index]] = i
        while ray.get(self.right_inputs[0].get_exhausted.remote()) == False:
            data = ray.get(self.right_inputs[1].get_next.remote())   
            self.right_attributes = data[-1] 
            right_attribute_index = self.right_attributes.index(self.right_join_attribute)
            self.right_attributes = list(self.right_attributes)
            self.right_attributes.pop(right_attribute_index)       
            self.right_attributes = tuple(self.right_attributes)
            for i in data:
                if i[right_attribute_index] in left_dict:
                    key = i[right_attribute_index]
                    i = list(i)
                    i.pop(right_attribute_index)
                    i = tuple(i)
                    self.joined_tuples.append(left_dict[key] + i)
        self.joined_tuples.append(left_attributes + self.right_attributes)
        logger.info("Join done")
        return list(self.joined_tuples)
            

    # Applies the operator logic to the given list of tuples
    def set_side(self, side):
        self.side = side

    def apply(self, tuples: List[ATuple]):
        self.log("Join operator {} is applying".format(self.id))
        if self.side == "left":
            attributes = tuples[-1]
            self.left_attributes = attributes
            # print(self.left_join_attribute)
            index = attributes.index(self.left_join_attribute)
            tuples = tuples[:-1] 
            for i in tuples:
                self.left_dict[i[index]] = i

            for key,val in self.right_dict.copy().items():
                if key in self.left_dict:
                    for i in val:
                        self.joined_tuples.append(self.left_dict[key]+i)
                    del self.right_dict[key]
            
        if self.side == "right":
            attributes = tuples[len(tuples)-1]
            index = attributes.index(self.right_join_attribute)
            attributes = list(attributes)
            attributes.remove(self.right_join_attribute)
            attributes = tuple(attributes)
            self.right_attributes = attributes
            tuples = tuples[:-1]
            for i in tuples:
                if i[index] in self.left_dict:
                    key = i[index]
                    i = list(i)
                    i.pop(index)
                    i = tuple(i)
                    self.joined_tuples.append(self.left_dict[key]+i)
                else:
                    if i[index] not in self.right_dict:
                        self.right_dict[i[index]] = [i]
                    else:
                        self.right_dict[i[index]].append(i)
            for key,val in self.right_dict.items():
                if key in self.left_dict:
                    for i in val:
                        self.joined_tuples.append(self.left_dict[key]+i)
                    del self.right_dict[key]
        # print("Right dict")
        # print(self.right_dict)  
        output = list(self.joined_tuples)
        output.append(tuple(self.left_attributes) + tuple(self.right_attributes))
        self.outputs[0].apply.remote(output)

# Project operator
@ray.remote
class Project(Operator):
    """Project operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        fields_to_keep (List(int)): A list of attribute indices to keep.
        If empty, the project operator behaves like an identity map, i.e., it
        produces and output that is identical to its input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes project operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[None],
                 fields_to_keep=[],
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        Operator.__init__(self, name="Project",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.fields_to_keep = fields_to_keep
        self.returns = []
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logging.info(msg)

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        self.log("Project operator {} is getting next batch of tuples".format(self.id))
        logger.info("Running project on the table..")
        data = ray.get(self.inputs[0].get_next.remote())
        headers = data[len(data)-1]
        index_list = [i for i in range(len(headers)) if headers[i] in self.fields_to_keep]
        output = []
        if len(data) > 0:
            for i in data:
                output.append(tuple([i[j] for j in index_list]))
        return output
    
    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        self.log("Project operator {} is applying".format(self.id))
        headers = tuples[len(tuples)-1]
        index_list = [i for i in range(len(headers)) if headers[i] in self.fields_to_keep]
        output = []
        if len(tuples) > 0:
            for i in tuples:
                output.append(tuple([i[j] for j in index_list]))
            if(self.outputs != None):
                self.outputs[0].apply.remote(output)
            else:
                self.returns.append(output)
        
    def get_returns(self):
        return self.returns[-1]

# Group-by operator
@ray.remote
class GroupBy(Operator):
    """Group-by operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        key (int): The index of the key to group tuples.
        value (int): The index of the attribute we want to aggregate.
        agg_fun (function): The aggregation function (e.g. AVG)
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes average operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 key,
                 value,
                 agg_gun,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        Operator.__init__(self, name="GroupBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        self.inputs = inputs
        self.outputs = outputs
        self.key = key
        self.value = value
        self.agg_fun = agg_gun
        self.aggregated_values = {} #will store the aggregated tuples
        self.output = [] #will store the output tuples
        self.count = 0
        self.sum = 0
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logging.info(msg)

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        self.log("GroupBy operator {} is getting next batch of tuples".format(self.id))
        aggregations = []
        data = ray.get(self.inputs[0].get_next.remote())
        if data is None:
            return None
        else:
            header = data[len(data)-1]
            self.key = header.index(self.key)
            for i in data[0:len(data)-1]:
                if i[self.key] not in self.aggregated_values:
                    self.aggregated_values[i[self.key]] = [k for k in data if k[self.key] == i[self.key]]
            if self.agg_fun != None and self.aggregated_values != {}:
                for i in self.aggregated_values.values():
                    index = header.index(self.value)
                    column, aggregation = self.agg_fun(i,index).get_next()
                    self.output.append(i[0]+(aggregation,))
                self.output.append(header+(column,))
                    
        return self.output

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        self.log("GroupBy operator {} is applying".format(self.id))
        if len(tuples) < 2:
            return
        else:
            header = tuples[len(tuples)-1]
            key = header.index(self.key)
            for i in tuples[0:len(tuples)-1]:
                if i[key] not in self.aggregated_values:
                    self.aggregated_values[i[key]] = [i]
                else:
                    if i not in self.aggregated_values[i[key]]:
                        self.aggregated_values[i[key]].append(i)
            if self.agg_fun != None:
                for k,i in enumerate(self.aggregated_values.values()):
                    index = header.index(self.value)
                    count = 0
                    sum = 0
                    for j in i:
                        count += 1
                        sum += int(j[index])
                    sum_c = (sum/len(i),) + (count,)
                    if(len(self.output) <= k):
                        self.output.append(j+sum_c)
                    else:
                        self.output[k] = (j+sum_c)

            header_row = header+("Average","Count")
            if(self.output[-1] != header_row):
                self.output.append(header_row)  
        self.outputs[0].apply.remote(self.output)
        self.outputs[0].inc.remote()

@ray.remote
class Sink:
    def __init__(self, key, outputs):
        self.key = key
        self.returns = []
        self.agg_dict = {}
        self.count = 0 
        self.last_indexes = []
        self.outputs = outputs
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logging.info(msg)
        
    
    def inc(self):
        self.count += 1
    
    def apply(self, tuples):
        self.log("Sink operator is applying")
        key_index = tuples[-1].index(self.key)
        header = tuples[-1]
        header = header[0:len(header)-1]
        if self.count % 2 == 0 or self.count == 1:
            for i in self.agg_dict.values():
                avg = i['sum']/i['count']
                self.returns.append((i['UID1'], i['UID2'], i['MID'], i['Rating'], avg),)
            self.returns.append(header)
            self.outputs[0].apply.remote(self.returns)
            self.returns = []
            self.agg_dict = {}
        for i in tuples[0:len(tuples)-1]:
            if i[key_index] not in self.agg_dict:
                self.agg_dict[i[key_index]] = {header[0]: i[0], header[1]: i[1], header[2]: i[2], header[3]: i[3], 'sum': i[4]*i[5], 'count': i[5]}
            else:
                self.agg_dict[i[key_index]]['sum'] += i[4]*i[5]
                self.agg_dict[i[key_index]]['count'] += i[5]

        

class Avg:
    def __init__(self, data, index):
        self.data = data
        self.index = index


    def get_next(self):
        data = self.data
        count = len(self.data)
        summ = 0
        for i in data:
            summ += int(i[self.index])
        return "Average", summ/count

    def finalize(self):
        pass
# Custom histogram operator
@ray.remote
class Histogram(Operator):
    """Histogram operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        key (int): The index of the key to group tuples. The operator outputs
        the total number of tuples per distinct key.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes histogram operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 key=0,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        Operator.__init__(self, name="Histogram",
                                        track_prov=track_prov,
                                        propagate_prov=propagate_prov,
                                        pull=pull,
                                        partition_strategy=partition_strategy)
        self.inputs = inputs
        self.outputs = outputs
        self.key = key
        self.histogram = {} #will store the aggregated tuples
        self.returns = [] #will store the output tuples
        self.count = 0 
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logging.info(msg)

    # Returns histogram (or None if done)
    def get_next(self):
        self.log("Histogram operator {} is getting next".format(self.id))
        data = ray.get(self.inputs[0].get_next.remote())
        if data is None:
            return None
        else:
            for i in data[0:len(data)-1]:
                if i[self.key] not in self.histogram:
                    self.histogram[i[self.key]] = 1
                else:
                    self.histogram[i[self.key]] += 1
            return list(self.histogram.items())

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        self.log("Histogram operator {} is applying".format(self.id))
        for i in tuples[0:len(tuples)-1]:
            if i[self.key] not in self.histogram:
                self.histogram[i[self.key]] = 1
            else:
                self.histogram[i[self.key]] += 1
        self.count += 1
        if self.count % 2 == 0 or self.count == 1:
            self.histogram = list(self.histogram.items())
            self.returns.append(self.histogram)
            self.histogram = {}
    
    def get_returns(self):
        return (self.returns[-1])
            
            

# Order by operator
@ray.remote
class OrderBy(Operator):
    """OrderBy operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        comparator (function): The user-defined comparator used for sorting the
        input tuples.
        ASC (bool): True if sorting in ascending order, False otherwise.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes order-by operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 comparator,
                 ASC=True,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        Operator.__init__(self, name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        self.inputs = inputs
        self.outputs = outputs
        self.comparator = comparator
        self.ASC = ASC
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logging.info(msg)
    # Returns the sorted input (or None if done)
    def get_next(self):
        self.log("OrderBy operator {} is getting next".format(self.id))
        data = ray.get(self.inputs[0].get_next.remote())
        header = data[len(data)-1]
        data = data[0:len(data)-1]
        sort_key = header.index(self.comparator)
        if self.ASC == True:
            data.sort(key=lambda x: x[sort_key])
        else:
            data.sort(key=lambda x: x[sort_key], reverse=True)
        
        return data+[header]

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        self.log("OrderBy operator {} is applying".format(self.id))
        header = tuples[len(tuples)-1]
        tuples = tuples[0:len(tuples)-1]
        sort_key = header.index(self.comparator)
        if self.ASC == True:
            tuples.sort(key=lambda x: x[sort_key])
        else:
            tuples.sort(key=lambda x: x[sort_key], reverse=True)
        self.outputs[0].apply.remote(tuples+[header])

# Top-k operator
@ray.remote
class TopK(Operator):
    """TopK operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes top-k operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 k=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        Operator.__init__(self, name="TopK",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.k = k
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logging.info(msg)

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        self.log("TopK operator {} is getting next".format(self.id))
        data = ray.get(self.inputs[0].get_next.remote())
        if self.k == None:
            return data
        else:
            header = data[len(data)-1]
            data = data[0:len(data)-1]
            data = data[0:self.k]
            data.append(header)
            return data
            
    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        self.log("TopK operator {} is applying".format(self.id))
        header = tuples[len(tuples)-1]
        tuples = tuples[0:len(tuples)-1]
        tuples = tuples[0:self.k]
        tuples.append(header)
        self.outputs[0].apply.remote(tuples)

# Filter operator
@ray.remote
class Select(Operator):
    """Select operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        predicate (function): The selection predicate.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes select operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 side,
                 predicate,
                 filter_column,
                 filter_value,
                 join_col,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        Operator.__init__(self, name="Select",
                                     track_prov=track_prov,
                                     propagate_prov=propagate_prov,
                                     pull=pull,
                                     partition_strategy=partition_strategy)
        self.inputs = inputs
        self.outputs = outputs
        self.predicate = predicate
        self.side = side
        self.filter_column = filter_column
        self.filter_value = filter_value
        self.join_col = join_col
        self.current_batch = []
        self.headers = []
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logging.info(msg)

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_headers(self):
        return self.headers
    
    def set_headers(self, headers):
        self.headers = headers
    
    def get_side(self):
        return self.side
    
    def set_side(self, side):
        self.side = side
    
    def get_next(self):
        self.log("Select operator {} is getting next".format(self.id))
        while ray.get(self.inputs[0].get_exhausted.remote()) == False:
            self.current_batch = []
            data1 = ray.get(self.inputs[0].get_next.remote())
            attributes = data1[len(data1)-1]
            if self.get_headers() == []:
                self.set_headers(attributes)
            if self.predicate != None:
                predicate = self.predicate(data = data1, att_index = attributes.index(self.filter_column), value = self.filter_value)
                self.current_batch = predicate.get_next()
                self.current_batch.append(attributes)
            else:
                self.current_batch = data1
            return self.current_batch
        
    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        self.log("Select operator {} is applying".format(self.id))
        headers = tuples[len(tuples)-1].tuple
        join_index = headers.index(self.join_col)
        tuples = tuples[0:len(tuples)-1]
        tuples = list(map(lambda x: x.tuple, tuples))
        if self.predicate != None:
            predicate = self.predicate(data = tuples, att_index = headers.index(self.filter_column), value = self.filter_value)
            self.current_batch = predicate.get_next()
        else:
            self.current_batch = tuples
        if self.current_batch != []:
            self.left_batch = [i for i in self.current_batch if int(i[join_index])%2 == 0]
            self.right_batch = [i for i in self.current_batch if int(i[join_index])%2 == 1]
        self.right_batch.append(headers)
        self.left_batch.append(headers)
        self.outputs[0].set_side.remote(self.side)
        self.outputs[0].apply.remote(self.left_batch)
        self.outputs[1].set_side.remote(self.side)
        self.outputs[1].apply.remote(self.right_batch)


class Equals:
    # Initializes equals operator
    def __init__(self, data, att_index, value):
        self.att_index = att_index
        self.value = value 
        self.data = data

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        matches = []
        for i in self.data:
            if i[self.att_index] == self.value:
                matches.append(i)
        
        return matches

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        pass


if __name__ == "__main__":


    parser = argparse.ArgumentParser(description='Task1')
    parser.add_argument('--query', type=str, default='1', help='Query to execute')
    parser.add_argument('--ff', type=str, help='The path to the file containing the friends relation')
    parser.add_argument('--mf', type=str, help='The path to the file containing the movies relation')
    parser.add_argument('--uid', default = 99, type=int, help='The user id')
    parser.add_argument('--mid', default = 99, type=int, help='The movie id')
    parser.add_argument('--pull', type=int, default=1, help='Whether to use pull-based (True) vs push-based (False) evaluation')
    parser.add_argument('--output', type=str, default='output', help='The path to the output file')
    args = parser.parse_args()


    file_path = args.ff
    file_path_2 = args.mf



    if args.query == '1' and args.pull == 1:
        logger.info("Executing query 1 (pull-based)")
        table_1 = Scan.remote(filepath = file_path, outputs=[Select], side = "left")
        table_2 = Scan.remote(filepath = file_path_2, outputs=[Select], side = "right")
        filtered_1 = Select.remote([table_1], [Join], None, Equals, filter_column="UID1", filter_value=str(args.uid), join_col = "")
        filtered_2 = Select.remote([table_2], [Join], None, Equals, filter_column="MID", filter_value=str(args.mid), join_col = "")
        joined = Join.remote([table_1, filtered_1], [table_2, filtered_2], [GroupBy], None, "UID2", "UID")
        avg = GroupBy.remote([joined], [TopK], "UID1", "Rating", Avg)
        project = Project.remote([avg], None, ["UID1", "Average"])
        data = ray.get(project.get_next.remote())
        with open(args.output, 'w') as f:
            csv_out=csv.writer(f)
            csv_out.writerow([data[-1][1]])
            for i in data[0:len(data)-1]:
                csv_out.writerow([i[1]])


    elif args.query == '1' and args.pull == 0:
        logger.info("Executing query 1 (push-based)")
        project = Project.remote(None, None, ["UID1", "Average"])
        sink = Sink.remote(key = "UID1", outputs = [project])
        avg = GroupBy.remote(None, [sink], "UID1", "Rating", "Avg")
        avg2 = GroupBy.remote(None, [sink], "UID1", "Rating", "Avg")
        join_push = Join.remote(None, None, [avg], None, "UID2", "UID", pull=False)
        join_push_2 = Join.remote(None, None, [avg2], None, "UID2", "UID", pull=False)
        filtered_1 = Select.remote(None, [join_push, join_push_2], None, Equals, filter_column="UID1", filter_value=str(args.uid), join_col = "UID2")
        filtered_2 = Select.remote(None, [join_push, join_push_2], None, Equals, filter_column="MID", filter_value=str(args.mid), join_col = "UID")
        table_1 = Scan.remote(filepath = file_path, outputs=[filtered_1], side = "left")
        table_2 = Scan.remote(filepath = file_path_2, outputs=[filtered_2], side = "right")
        ray.get(table_1.start.remote())
        ray.get(table_2.start.remote())
        time.sleep(5)
        data = ray.get(project.get_returns.remote())
        with open(args.output, 'w') as f:
            csv_out=csv.writer(f)
            csv_out.writerow([data[-1][1]])
            for i in data[0:len(data)-1]:
                csv_out.writerow([i[1]])
    


    if args.query == '2' and args.pull == 1:
        logger.info("Executing query 2 (pull-based)")
        table_1 = Scan.remote(filepath = file_path, outputs=[Select], side = "left")
        table_2 = Scan.remote(filepath = file_path_2, outputs=[Select], side = "right")
        filtered_1 = Select.remote([table_1], [Join], None, Equals, filter_column="UID1", filter_value=str(args.uid), join_col = "")
        filtered_2 = Select.remote([table_2], [Join], None, None, filter_column=None, filter_value=None, join_col = "")
        joined = Join.remote([table_1, filtered_1], [table_2, filtered_2], [GroupBy], None, "UID2", "UID")
        avg = GroupBy.remote([joined], [TopK], "MID", "Rating", Avg)
        order_by = OrderBy.remote([avg], [TopK], "Average", False)
        top_k = TopK.remote([order_by], [Project], 1)
        project = Project.remote([top_k], [None], ["MID", "Average"])
        project2 = Project.remote([project], [None], ["MID"])
        data = ray.get(project2.get_next.remote())
        with open(args.output, 'w') as f:
            csv_out=csv.writer(f)
            csv_out.writerow([data[-1][0]])
            for i in data[0:len(data)-1]:
                csv_out.writerow([i[0]])

    elif args.query == '2' and args.pull == 0:
        logger.info("Executing query 2 (push-based)")
        project_2 = Project.remote(None, None, ["MID"])
        project_1 = Project.remote(None, [project_2], ["MID", "Average"])
        top_k = TopK.remote(None, [project_1], 1)
        order_by = OrderBy.remote(None, [top_k], "Average", False)
        sink = Sink.remote(key = "MID", outputs = [order_by])
        avg = GroupBy.remote(None, [sink], "MID", "Rating", Avg)
        avg2 = GroupBy.remote(None, [sink], "MID", "Rating", Avg)
        join_push = Join.remote(None, None, [avg], None, "UID2", "UID", pull=False)
        join_push_2 = Join.remote(None, None, [avg2], None, "UID2", "UID", pull=False)
        filtered_2 = Select.remote(None, [join_push, join_push_2], None, None, filter_column=None, filter_value=None, pull=False, join_col = "UID")
        filtered_1 = Select.remote(None, [join_push, join_push_2], None, Equals, filter_column="UID1", filter_value=str(args.uid), join_col = "UID2")
        table_1 = Scan.remote(filepath = file_path, outputs=[filtered_1], side = "left")
        table_2 = Scan.remote(filepath = file_path_2, outputs=[filtered_2], side = "right")
        ray.get(table_1.start.remote())
        ray.get(table_2.start.remote())
        time.sleep(5)
        data = ray.get(project_2.get_returns.remote())
        with open(args.output, 'w') as f:
            csv_out=csv.writer(f)
            csv_out.writerow([data[-1][0]])
            for i in data[0:len(data)-1]:
                csv_out.writerow([i[0]])



    if args.query == '3' and args.pull == 1:
        logger.info("Executing query 3 (pull-based)")
        table_1 = Scan.remote(filepath = file_path, outputs=[Select], side = "left")
        table_2 = Scan.remote(filepath = file_path_2, outputs=[Select], side="right")
        filtered_1 = Select.remote([table_1], [Join], None, Equals, filter_column="UID1", filter_value=str(args.uid), join_col = "")
        filtered_2 = Select.remote([table_2], [Join], None, Equals, filter_column="MID", filter_value=str(args.mid), join_col = "")
        joined = Join.remote([table_1, filtered_1], [table_2, filtered_2], [Histogram], None, "UID2", "UID")
        hist = Histogram.remote([joined], [Project], 3)
        data = ray.get(hist.get_next.remote())
        with open(args.output, 'w') as f:
            csv_out=csv.writer(f)
            csv_out.writerow(["UID", "explanation"])
            for i in data[0:len(data)]:
                csv_out.writerow(i)

    
    elif args.query == '3' and args.pull == 0:
        logger.info("Executing query 3 (push-based)")
        hist = Histogram.remote(None, None, 3)
        join_push_2 = Join.remote(None, None, [hist], None, "UID2", "UID", pull=False)
        join_push = Join.remote(None, None, [hist], None, "UID2", "UID", pull=False)
        filtered_1 = Select.remote(None, [join_push, join_push_2], None, Equals, filter_column="UID1", filter_value=str(args.uid), join_col = "UID2")
        filtered_2 = Select.remote(None, [join_push, join_push_2], None, Equals, filter_column="MID", filter_value=str(args.mid), pull=False, join_col = "UID")
        table_1 = Scan.remote(filepath = file_path, outputs=[filtered_1], side = "left")
        table_2 = Scan.remote(filepath = file_path_2, outputs=[filtered_2], side = "right")
        ray.get(table_1.start.remote())
        ray.get(table_2.start.remote())
        time.sleep(10)
        data = ray.get(hist.get_returns.remote())
        with open(args.output, 'w') as f:
            csv_out=csv.writer(f)
            csv_out.writerow(["UID", "explanation"])
            for i in data[0:len(data)]:
                csv_out.writerow(i)
