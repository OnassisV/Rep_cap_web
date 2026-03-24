"""Inicializacion del proyecto Django con soporte PyMySQL para Railway."""

import pymysql


# Django 6 exige que el backend MySQL reporte una version tipo mysqlclient>=2.2.1.
# PyMySQL actua como reemplazo de MySQLdb, asi que exponemos una version compatible.
pymysql.version_info = (2, 2, 1, "final", 0)
pymysql.__version__ = "2.2.1"

pymysql.install_as_MySQLdb()
