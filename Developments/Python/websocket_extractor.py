import json, hmac, hashlib, time, base64, yaml, os
import asyncio
import websockets
import sys
from datetime import datetime
from azure.storage.blob import ContainerClient
from flatten_json import flatten

class WebSocket:

    def __init__(self,portfolio_id):
        config=self.load_config()

        self.connect_str=config['connect_str']
        self.container_name=config['container_name']
        self.uri = config['uri']
        self.PASSPHRASE = config['PASSPHRASE']
        self.ACCESS_KEY = config['ACCESS_KEY']
        self.SIGNING_KEY = config['SIGNING_KEY']
        self.SVC_ACCOUNTID = config['SVC_ACCOUNTID']
        self.portfolio_id=portfolio_id

        s = time.gmtime(time.time())
        self.TIMESTAMP = time.strftime("%Y-%m-%dT%H:%M:%SZ", s)

    def load_config(self):
        dir_root= os.path.dirname(os.path.abspath(__file__))
        with open(dir_root + "/config.yaml","r") as yamlfile:
            return yaml.load(yamlfile,Loader=yaml.FullLoader)


    async def sign(self,channel, key, secret, account_id, portfolio_id, product_ids):
        message = channel + key + account_id + self.TIMESTAMP + portfolio_id + product_ids
        print(message)
        signature = hmac.new(secret.encode('utf-8'), message.encode('utf-8'), digestmod=hashlib.sha256).digest()
        signature_b64 = base64.b64encode(signature).decode()
        return signature_b64


    def upload_responde(self,response):
        now=datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour
        hour_path=f'{year}/{month}/{day}/{hour}'

        try:
            # Create the Blob Client objects
            print('Uploading files to blob storage...')
            container_client=ContainerClient.from_connection_string(self.connect_str,self.container_name+'/'+hour_path)
            blob_client=container_client.get_blob_client(f'{now}.json')
            blob_client.upload_blob(response,overwrite=True)

        except Exception as e:
            print(e)

    async def main_loop(self):
        async with websockets.connect(self.uri, ping_interval=60, max_size=None) as websocket:
            signature = await self.sign('orders', self.ACCESS_KEY, self.SIGNING_KEY, self.SVC_ACCOUNTID, self.portfolio_id, "BTC-USD")
            print(signature)
            auth_message = json.dumps({
                "type": "subscribe",
                "channel": "orders",
                # "channel" : "l2_data",
                "access_key": self.ACCESS_KEY,
                "api_key_id": self.SVC_ACCOUNTID,
                "timestamp": self.TIMESTAMP,
                "passphrase": self.PASSPHRASE,
                "portfolio_id" : self.portfolio_id,
                "signature": signature,
                "product_ids": ["BTC-USD"]
            })
            
            await websocket.send(auth_message)
            try:
                processor = None
                while True:
                    response = await websocket.recv()
                    parsed = json.loads(response)
                    print(json.dumps(parsed,indent=4))
                    if list(parsed['events'][0].keys())[0] =='subscriptions':
                        pass
                    else:
                        flat_json = flatten(parsed)
                        new_json={}
                        for key in flat_json.keys():
                            if key.startswith('events_0_type'):
                                key=key.replace('events_0_','')
                                new_json[key]=flat_json[f'events_0_{key}']
                            elif key.startswith('events_0_orders_0_'):
                                key=key.replace('events_0_orders_0_','')
                                new_json[key]=flat_json[f'events_0_orders_0_{key}']
                            else:
                                new_json[key]=flat_json[f'{key}']
                        parsed= json.dumps(new_json)
                        self.upload_responde(parsed)
            except websockets.exceptions.ConnectionClosedError:
                print(f"{datetime.now()}: Error caught")
                sys.exit(1)


websocket=WebSocket('92451425-e4b9-41b6-9dd0-90daea997261')
asyncio.run(websocket.main_loop())