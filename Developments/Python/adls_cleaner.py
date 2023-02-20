from azure.storage.blob import BlobServiceClient,BlobClient,ContainerClient
import yaml,os,sys


def load_config():
    dir_root= os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config.yaml","r") as yamlfile:
        return yaml.load(yamlfile,Loader=yaml.FullLoader)

directory_path = str(sys.argv[1])
print('path to clean: ',directory_path)
path_list=list(directory_path.split('/'))

config=load_config()
blob_service_client  = BlobServiceClient.from_connection_string(config['connect_str'])
container_client = blob_service_client.get_container_client(path_list[0])

del path_list[0]
separator = "/"
path = separator.join(path_list)
adls_account="put here your adls account"

blobs_list = container_client.walk_blobs(name_starts_with=path+'/')

for blob in blobs_list:
    blob_name=blob.name
    blob_name=blob_name.replace(path+'/','')
    if '/' not in blob_name:
        if '.' in blob_name:
            print(blob_name)
            copied_blob = blob_service_client.get_blob_client(f'{directory_path}/old', blob_name)
            source_blob = f"{adls_account}/{directory_path}/{blob_name}"
            copied_blob.start_copy_from_url(source_blob)

            remove_blob = blob_service_client.get_blob_client(directory_path, blob_name)
            remove_blob.delete_blob()