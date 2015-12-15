import boto3

client = boto3.client('glacier')
vault = 'BaculaTest001'

job_id = client.initiate_job(vault, {"Description":"inventory-job", "Type":"inventory-retrieval", "Format":"JSON"})
 
print("inventory job id: %s"%(job_id,));

