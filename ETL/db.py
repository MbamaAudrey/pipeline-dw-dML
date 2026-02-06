import psycopg2

def get_connection(cfg):
    return psycopg2.connect(**cfg)
