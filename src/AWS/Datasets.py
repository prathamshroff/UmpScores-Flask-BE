import pandas as pd
import os
import boto3
from decimal import Decimal
import botocore
import time

class Table():
    """
    The Dataset class is our wrapper around boto3's dynamodb object. We will
    use it to add more robust and safe queries in the future. 

    Parameters
    ----------
    iam_role : dict
        A dictionary containing the iam role for the corresponding 
        dynamodb table. Should look as follows:
            e.g. {
                "key": iam_access_key,
                "secret": iam_secret_key 
            }
    table : str
        A string representing the dynamodb table to use given the iam_role.
            e.g. "Players"

    Attributes
    ----------
    iam_role : dict
        This is where we store iam_role
    dynamodb : boto3.resource object
        Boto3 resoure object for dynamodb. Uses iam_role and the table parameter
        to connect to dynamodb from aws. We use this object for all interactions
        with dynamodb.
    """
    RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException', 
        'ThrottlingException')
    def __init__(self, iam_role, table, cloudsearch=None):
        self.iam_role = iam_role
        self.__table_name = table
        self.dynamodb = boto3.resource('dynamodb',
            aws_access_key_id=self.iam_role['key'],
            aws_secret_access_key=self.iam_role['secret'],
            region_name='us-east-1'
            ).Table(table)
        self.client = boto3.client('dynamodb',
            aws_access_key_id = self.iam_role['key'],
            aws_secret_access_key = self.iam_role['secret'],
            region_name='us-east-1')
        self.cloudsearch = cloudsearch

    @staticmethod
    def fillna(df, string_fields):
        """
        Fills in empty fields with -1 for number values and 'n/a' for strings
        """
        values = {key: 'n/a' for key in string_fields}

        number_fields = list(df.columns)
        for column in string_fields:
            if column in number_fields:
                number_fields.remove(column)

        values.update({key: -1 for key in number_fields})

        df = df.fillna(value=values)
        return df

    @staticmethod
    def drop_y(df):
        """
        Removes duplicate columns which emerged from merging pd.DataFrames

        Parameters
        ----------
        df : pd.DataFrame
            some table which has recently been merged with another pd.DataFrame

        Returns
        ----------
        pd.DataFrame
            returns a DataFrame which removed all potential duplicate columns
        """
        to_drop = [x for x in df if x.endswith('_y')]
        df.drop(to_drop, axis=1, inplace=True)
        return df

    def get(self, query_map, **kwargs):
        """
        Get method takes some query map and some filter options
        then returns a json object representing that entire item entry within dynamodb

        Parameters
        ----------
        queryMap : dict
                Query the dynamodb table for the following key value pairs. Make
                sure you give the primary/partition key as a key value pair within
                this dictionary.
                        e.g. {"Umpires": "jordan baker", ...}
        FilterExpressions : boto3.dynamodb.conditions.(Attr/Key)
                Filter the results given some attribute/key filter option. If None,
                FilterExpressions will not be sent.
                        e.g. Attr('timeStamp').between(start, end)

        Returns
        ----------
        dict
                Matched item element within the database. If no such element exists returns
                empty dict. Response represents an entire element within the table. For
                example, if this dataset is connected to the Umpires Table, the return
                would be a json response of all the statistics about a singular umpire.
                        e.g. {
                                "attr1": "val1",
                                "attr2": "val2",
                                ...
                        }

        """
        data = self.dynamodb.get_item(Key = query_map, **kwargs)
        if 'Item' not in data:
            return {}
        return data['Item']

    def scan(self, **kwargs):
        """
        Scan method takes some query keyword and some filter options
        then returns a json response consisting of relevant dynamodb
        elements. Uses a paginator to effectively iterate through every item

        Returns
        ----------
        list of dictionaries
            Matched item elements within the database. If no such elements exists returns
            empty dict. One dictionary represents an entire item within dynamodb.
            For example, if this dataset is connected to the Umpires table, one dictionary
            would give all the statistics about a single Umpire.
                e.g. [{
                    "attr1": "val1",
                    "attr2": "val2",
                    ...
                }, 
                ...]

        """
        response = self.dynamodb.scan(**kwargs)
        data = response['Items']
        iteration = 0
        total_time = 0
        while 'LastEvaluatedKey' in response: 
            now = time.time()
            response = self.dynamodb.scan(**kwargs, ExclusiveStartKey=response['LastEvaluatedKey'])
            local_time = time.time() - now
            total_time += local_time
            print('{0} iteration {1} loading new page in {2}s'.format(self.__table_name, 
                iteration, local_time))
            iteration +=1
            data.extend(response['Items'])
        print(total_time)
        return data

    def query(self, **kwargs):
        resp = self.dynamodb.query(**kwargs)
        if 'Items' in resp:
            return resp['Items']
        else:
            return []
        
    def upload(self, refined_filepath, backoff_init = 50, exp_backoff = False): 
        """
        Uploads every item within some filepath to this dynamodb table

        Parameters
        ----------
        refined_filepath : str
            string representing the relative path to some data file
        backoff_init
            initial seed for our backoff value. Upon a throttle or throughput
            exception which we get from having too high of a request wait, we exponentiate
            backoff and wait for new backoff durations to reduce throughput rate.
        exp_backoff : bool
            if true, will use exponentially increasing waits in between requests. If false, will use
            linearly increasing waits.
        """ 
        df = pd.read_csv(refined_filepath, keep_default_na=False)
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
        data = df.to_dict()
        keys = list(data.keys())
        
        # data['number'] = [5, 4, 3, 78, ...] which is an array of values for every row
        for item_id in range(len(data[keys[0]])):
            item = {key: data[key][item_id] for key in keys}
            for key in keys:
                if item[key] == '':
                    item[key] = -1
                if type(item[key]) == float or type(item[key]) == int:
                    item[key] = Decimal(str(item[key]))
            backoff = backoff_init
            while True:
                try:
                    # cloudsearch cache just means new items were added to dynamodb
                    # therefore we need to add them to cloudsearch
                    self.dynamodb.put_item(
                        Item=item
                    )
                    if self.cloudsearch != None:
                        self.cloudsearch.cache.append(item)
                    
                    if backoff > backoff_init:
                        if exp_backoff:
                            backoff /= 2
                        else:
                            backoff -= backoff_init
                    break
                except botocore.exceptions.ParamValidationError as e:
                    print("Wrong Item type")
                    print(e)
                    exit(0)
                except botocore.exceptions.ClientError as e:
                    errcode = e.response['Error']['Code']
                    if errcode in Table.RETRY_EXCEPTIONS:
                        time.sleep(backoff / 1000)
                        if exp_backoff:
                            backoff *= 2
                        else:
                            backoff += backoff_init
                        if (backoff / 1000 > 60):
                            print('Exponential Backoff Failed. Load too heavy!')
                            exit(1)
                        else:
                            print('Increasing backoff to {0}'.format(backoff))
                    elif errcode == 'ValidationException':
                        print(e)
                        print(item)
                        exit(1)
                    else:
                        print(e)
                        exit(1)
                except Exception as e:
                    print("Unknown Error {0}".format(e))
                    return


        print('Dynamodb table for {0} refreshed'.format(self.__table_name))

    @staticmethod
    def unfoil(arr):
        """
        Removes 'S' and 'N' fields from AWS responses
        """
        for hashmap in arr:
            for key in hashmap:
                data_type = list(hashmap[key].keys())[0]
                hashmap[key] = hashmap[key][data_type]
        return arr

    def batch_get(self, keys, batch_size = 100):
        """
        Gathers multiple key value pairs from this table. Upon throughput throttling,
        retries request. 
        """
        if keys == []:
            return []

        data = []
        workers = []
        i = 0

        while i < len(keys):
            new_i = i + batch_size
            workers.append(keys[i:new_i])
            i = new_i

        remaining_keys = []

        for batch in workers:
            page = self.client.batch_get_item(RequestItems = {
                self.__table_name: {
                    'Keys': batch
                }
            })
            if 'Responses' in page:
                data += Table.unfoil(page['Responses'][self.__table_name])
            if page['UnprocessedKeys'] != {}:
                print('Found UnprocessedKeys in {0}'.format(self.__table_name))
                remaining_keys += Table.unfoil(page['UnprocessedKeys'][self.__table_name]['Keys'])
        return data + self.batch_get(remaining_keys, batch_size = batch_size)

    def clear(self, primary_key, sort_key = None, backoff_init = 50, exp_backoff = False):
        """
        Completely deletes every item within this dynamodb table

        Parameters
        ----------
        primary_key : str
            string representing the primary key for this dynamodb table
        sort_key : str
            string representing the sort key for this dynamodb table. If 
            no sort key exists leave as None
        backoff_init : int
            initial seed for our backoff value. Upon a throttle or throughput
            exception which we get from having too high of a request wait, we exponentiate
            backoff and wait for new backoff durations to reduce throughput rate.
        exp_backoff : bool
            if true, will use exponentially increasing waits in between requests. If false, will use
            linearly increasing waits.
        """ 
        paginator = self.client.get_paginator('scan')
        operation_parameters = {
            'TableName': self.__table_name
        }
        page_iterator = paginator.paginate(**operation_parameters)
        for scan in page_iterator:
            backoff = backoff_init
            while True:
                try:
                    with self.dynamodb.batch_writer() as batch:
                        for each in scan['Items']:

                            primary_val = Decimal(each[primary_key]['N']) if 'N' in each[primary_key] \
                                else each[primary_key]['S']

                            if sort_key == None:
                                key = {
                                    'Key': {
                                        primary_key: primary_val
                                    }
                                }
                            else:
                                sort_val = Decimal(each[sort_key]['N']) if 'N' in each[sort_key] \
                                    else each[sort_key]['S']
                                key = {
                                    'Key': {
                                        primary_key: primary_val,
                                        sort_key: sort_val
                                    }
                                }
                                batch.delete_item(**key)
                                if backoff > backoff_init:
                                    if exp_backoff:
                                        backoff /= 2
                                    else:
                                        backoff -= backoff_init
                                print('Deleted pk: {0}, sk: {1}'.format(primary_val, 
                                    sort_val))
                    break
                except botocore.exceptions.ClientError as e:
                    errcode = e.response['Error']['Code']
                    if errcode in Table.RETRY_EXCEPTIONS:
                        time.sleep(backoff / 1000)
                        if exp_backoff:
                            backoff *= 2
                        else:
                            backoff += backoff_init
                        if (backoff / 1000 > 60):
                            print('Exponential Backoff Failed. Load too heavy!')
                            exit(0)
                        else:
                            print('Increasing backoff to {0}'.format(backoff))
                    elif errcode == 'ValidationException':
                        print('Incorrect primary or sort key, used')
                        return
                    else:
                        print(e)
                except Exception as e:
                    print('Uncaught super exception: {0}'.format(e))
        print('Dynamodb table for {0} emtpied'.format(self.__table_name))

    def __str__(self):
        return '{0}'.format(self.__table_name)

