bind = "0.0.0.0:10000"
workers = 1
threads = 4
timeout = 120
graceful_timeout = 20
accesslog = "-"
errorlog = "-"

def post_worker_init(worker):
    import app as application
    import hardening
    hardening.install(application)
