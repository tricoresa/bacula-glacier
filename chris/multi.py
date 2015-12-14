import boto3

chunksize=8388608

glac = boto3.resource('glacier')
vault = glac.Vault('117778811131','BaculaTest001')

init = vault.initiate_multipart_upload(vaultName='BaculaTest001', archiveDescription='/storage/sd00/ckerns-rhel6/fs/ckerns-rhel6_IncrVol15', partSize = str(chunksize))

multipart_upload = vault.MultipartUpload(init.id)

f = file('/storage/sd00/ckerns-rhel6/fs/ckerns-rhel6_IncrVol15', buffering=chunksize)
chunk = f.read(chunksize)

response = multipart_upload.upload_part(range='bytes 0-8388607/*', body=chunk)
print response

