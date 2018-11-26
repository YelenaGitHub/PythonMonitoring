# Jupyter notebook
# StreamSets Rest API historical data loads to ElasticSearch
# coding: utf-8

# In[3]:

import json
import pandas as pd
import elasticsearch
from pandas.io.json import json_normalize

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


# In[ ]:
# Set Elastic host and port parameters
# In[16]:

es = Elasticsearch([{'host':'<ElasticHost>', 'port': '<ElasticPort>'}])

# In[ ]:
# Set path to StreamSets Rest API historical data in json
# In[ ]:

pipeline_history = pd.read_json('<PathToJson>')


# In[ ]:
# Set country name
# In[5]:

pipeline_history['country']='<CountryName>'

# In[6]:

empty_metrics = pipeline_history[pipeline_history.metrics.isna()]
empty_metrics

# In[8]:

empty_metrics = pipeline_history[pipeline_history.metrics.isna()]
empty_metrics
empty = pipeline_history[pipeline_history.metrics.isna()]
empty['input_rec'] = 0
empty['output_rec'] = 0
empty['batch_err_rec'] = 0
empty['batch_err_msg'] = 0

# In[9]:

not_empty = pipeline_history[~pipeline_history.metrics.isna()]

# In[10]:

not_empty = pipeline_history[~pipeline_history.metrics.isna()]
not_empty['input_rec'] = not_empty['metrics'].apply(lambda x: json_normalize(json.loads(x), errors='ignore')['counters.stage.HadoopFS.inputRecords.counter.count'])
not_empty['output_rec'] = not_empty['metrics'].apply(lambda x: json_normalize(json.loads(x), errors='ignore')['counters.pipeline.batchOutputRecords.counter.count'])
not_empty['batch_err_rec'] = not_empty['metrics'].apply(lambda x: json_normalize(json.loads(x), errors='ignore')['counters.pipeline.batchErrorRecords.counter.count'])
not_empty['batch_err_msg'] = not_empty['metrics'].apply(lambda x: json_normalize(json.loads(x), errors='ignore')['counters.pipeline.batchErrorMessages.counter.count'])

# In[11]:

result = pd.concat([not_empty, empty])
result = result.fillna(0)

# In[12]:

result['timestamp'] = result['timeStamp']
documents = result[['pipelineId', 'country', 'name', 'message', 'retryAttempt', 'status', 'timestamp','input_rec','output_rec', 'batch_err_rec','batch_err_msg']].to_dict(orient='records')

# In[17]:

es.indices.create(index='dicestreams_test',body={})

# In[20]:

bulk(es, documents, index='dicestreams_test',doc_type='country', raise_on_error=False)