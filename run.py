import os,glacier
fname = '/storage/sd00/ckerns-rhel6/fs/ckerns-rhel6_DiffVol8'
chunksize = 8388608
r = 0
fsize = os.stat(fname).st_size
vid = 'NLjKaw4V7QCxN2umM0Y4wqn5MFo7ZscWPKGt6I8nwNh6VMcgtAoCAejLxCKeSOCMkilWOJabVuutc1QQMvL6zkgHwqu3'

with open (fname, 'rb') as f:
    for chunk in iter(lambda: f.read(int(chunksize)), b''):
        if r + int(chunksize) > fsize:
            incr = fsize - r
        else:
            incr = chunksize
        up = glacier.upload_part('BaculaTest001',vid, range="bytes " + str(r) + "-" + str(r + int(incr) - 1) + "/*",body=chunk)
        r = r + chunksize
	print up
