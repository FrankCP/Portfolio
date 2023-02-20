import os
import yaml
import requests,json
from datetime import datetime,timedelta
from azure.storage.blob import ContainerClient
import openpyxl

def load_config():
    dir_root= os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config.yaml","r") as yamlfile:
        return yaml.load(yamlfile,Loader=yaml.FullLoader)

class Excel_File():

    def __init__(self):

        config=load_config()
        self.connect_str=config['connect_str']
        self.container_name=config['container_name']
        self.file_name=config['file_name']
        self.local_path=config['local_path']
        self.file_path=os.path.join(self.local_path,self.file_name)+'.xlsm'
        self.now = datetime.now()
        self.year = self.now.year
        self.month = self.now.month
        self.day = self.now.day
        self.hour = self.now.hour
    
    def api_request(self):
        url = "https://api.coinbase.com/v2/exchange-rates?currency=BTC"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)

        json_data = json.loads (response.text)
        print('current btc price: ',((json_data['data'])['rates'])['USD'])
        return float(((json_data['data'])['rates'])['USD'])

    def hashrate_api(self):
        url = 'https://blockchain.info/q/hashrate'
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        json_data = json.loads (response.text)
        print('current hash rate: ', json_data/1000000)

        return int(json_data/1000000)

    def download(self):

        # Download the blob to a local file
        edf_file='Put here your xlsm file'
        edf_path=os.path.join(self.local_path,edf_file)

        container_client2 = ContainerClient.from_connection_string( self.connect_str,self.container_name)
        print("\nDownloading blob to \n\t" + edf_path)
    
        with open(file=edf_path, mode="wb") as download_file2:
            download_stream2=container_client2.download_blob(edf_file)
            download_file2.write(download_stream2.readall())

    def modify(self):
        
        wb=openpyxl.load_workbook(self.file_path,keep_vba=True)
        sh1=wb['Bitcoin Mining Economics']

        sh1['C21']=self.api_request()
        sh1['C20']=self.hashrate_api()

        tomorrow=self.now+timedelta(days=1)
        t_year=tomorrow.year
        t_month=tomorrow.month
        t_day=tomorrow.day

        sh2=wb['EB']
        sh2['A3']=str(t_month)+'/'+str(t_day)+'/'+str(t_year)

        wb.save(self.file_path)

    def upload(self):

        try:
            # Create the Blob Client objects
            print('Uploading file to blob storage...')

            container_client=ContainerClient.from_connection_string(self.connect_str,self.container_name)
            blob_client=container_client.get_blob_client(self.file_name+'.xlsm')
            
            with open(self.file_path,'rb') as data:
                blob_client.upload_blob(data,overwrite=True)
        
        except Exception as e:
            print(e)
        
        self.request_logicapp(f'The Day Ahead Power- Form was updated and sent at {self.now} UTC')
    
    def request_logicapp(self,message):
        logic_app_url = "Put here your logic app URL"
        
        obj = {
            "message" : f"{message}"
        }
        
        json_dump = json.dumps(obj)
        json_obj = json.loads(json_dump)
        
        requests.post(logic_app_url, json=json_obj)

excel_file=Excel_File()


try:
    excel_file.download()
    excel_file.modify()
    excel_file.upload()

    #excel_file.hashrate_api()
except Exception as e:
    print(e)