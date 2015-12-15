import os,glacier
fname = '/storage/sd00/ckerns-rhel6/fs/ckerns-rhel6_DiffVol8'
chunksize = 8388608
r = 0
fsize = os.stat(fname).st_size
vid = 'y-FwnC7dhXVHYY2safxK1f3ZEtJ79aHLVOwmztFk18yJ_h9xRHufrJ5isH4Z6f7vCxzzvs9VLzsNzEdfrX1hBhdS_K_X'

with open (fname, 'rb') as f:
    for chunk in iter(lambda: f.read(int(chunksize)), b''):
        if r + int(chunksize) > fsize:
            incr = fsize - r
        else:
            incr = chunksize
        glacier.upload_part('BaculaTest001',vid, range="bytes " + str(r) + "-" + str(r + int(incr) - 1) + "/*",body=chunk)
        r = r + chunksize

