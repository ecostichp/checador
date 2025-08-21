# Checador

### Para crear el entorno python de trabajo en el proyecto:  

##### 1. Actualiza PIP global.
```
python -m pip install -U Pip  
```

##### 2. Crea el entorno para el proyecto (lo llamaremos 'env').
```
python -m venv env_checador
env_checador\Scripts\activate
```

##### 3. Actualiza PIP en tu entorno e instala las dependencias del proyecto.
```
python -m pip install -U Pip
python -m pip install -r requirements.txt
```

##### 3. No hay paquetes locales para instalar.
Si en un futuro se requiere instalar de manera local y editable el paquete IACele:

Desde la carpeta raiz '\checador\' corre el siguiente script:

```
python -m pip install -e .\local_packages\iacele_package\
```