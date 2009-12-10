=====
Postgresql settings
===== 
For importing one of the larger Wikipedia dumps, the settings of
Postgresql in your postgresql.conf should be adjusted. The following settings are just a rule of
thumb, you should think for yourself which values fit your system best!

Memory
------

shared_buffers = 256-1024MB
temp_buffers = 8-16MB
work_mem = 8-16MB
maintenance_work_mem = 256-512MB

logging
-------
log_statement = 'none'

SSL
---

A SSL connection may break the import routine, it is safe to turn it
off:

ssl=false
