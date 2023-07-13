# Prueba técnica - José Luis Pérez Torres
## Framework dirigido por metadatos

## 1. Guía de instalación y uso
Versión de Python: Python 3.8

1. Instalar dependencias: pipenv install
2. Activar entorno virtual: pipenv shell
3. Ejecutar: python main.py --n_processes < nº de procesos simultáneos >

El fichero de metadatos se debe incluir en la ruta data/metadata.json

## 2. Estructura del proyecto

````text
.
├── data
│   ├── input
│   │   └── events
│   │       └── person
│   │           └── input.json
│   ├── metadata.json
│   └── output
│       ├── discards
│       │   └── person
│       │       └── raw-ko.json
│       └── events
│           └── person
│               └── raw-ok.json
├── main.py
└── metadata_framework
    ├── __init__.py
    ├── core
    │   ├── __init__.py
    │   ├── extract_service.py
    │   ├── load_service.py
    │   └── transform_service.py
    ├── entities
    │   ├── __init__.py
    │   ├── data_container.py
    │   ├── dataflow.py
    │   ├── generate.py
    │   ├── sink.py
    │   └── transformation.py
    └── log
        ├── __init__.py
        ├── log.py
        └── logger_agent.py


````
- **core**: Módulo con el código principal de la ETL. En **transform_services.py** se encuentra el **transformation_catalog**.
- **entities**: Clases que representan las entidades del fichero de metadatos.
- **log**: Módulo de log.
## 3. Configuración del fichero de metadatos

En un fichero de metadatos JSON pueden ir incluidos diferentes **dataflows**. Cada dataflow debe incluir los campos:

1. **name**
2. **sources** ⮕ Rutas e información de los ficheros de entrada del dataflow
3. **transformations** ⮕ Debe incluir al menos una validación y una transformación
4. **sinks** ⮕ Lugares de ingesta de los datos. Obligatoriamente debe exitir un sink de ficheros erróneos.

### 3.1 Sources
Un ejemplo del cuerpo de sources es:

```text
    "sources": [{
                "name": "person_inputs",
                "path": "data/input/events/person/",
                "format": "JSON"
                }]
```
- **name**: nombre del source.
- **path**: ruta del archivo de entrada.
- **format**: formato del archivo de entrada.

### 3.2 Transformations

Un ejemplo del cuerpo de transformations es:

```text
"transformations": [{
                "name": "validation",
                "type": "validate_fields",
                "params": {
                    "input": "person_inputs",
                    "validations": [{
                        "field": "office",
                        "validations": ["notEmpty"]
                                    },
                                    {
                        "field": "age",
                        "validations": ["notNull"]
                                    }]
                           }
                    },
                    {
                "name": "ok_with_date",
                "type": "add_fields",
                "params": {
                    "input": "validation_ok",
                    "addFields": [{
                    "name": "dt",
                    "function": "current_timestamp"
                                 }]
                         }
                    }]
```

- **Validaciones**: deben incluir nombre, tipo = validate_fields, input, field (campo que queremos validar) y validations (validaciones que se llevan a cabo). Se debe crear una estructura de validation para cada source si se desea.

- **Transformaciones**: solo se aplican las transformaciones (addFields) a los sources que han sido validados (validation_ok). Deben incluir nombre, tipo e input. A continuación se pueden añadir tantos campos transformados como se desee. Para ello se debe incluir name, function y opcionalmente fields y additional si la función los necesita.

### 3.3 Sinks

Un ejemplo del cuerpo de sinks es:

```text
"sinks": [{
            "input": "person_inputs",
            "name": "raw-ok",
            "paths": [
                "data/output/events/person/"
                    ],
            "format": "JSON",
            "saveMode": "OVERWRITE"
            },
            {
            "input": "validation_ko",
            "name": "raw-ko",
            "paths": [
                "data/output/discards/person/"
                    ],
            "format": "JSON",
            "saveMode": "OVERWRITE"
        }]
```
- **input**: nombre original del data source.
- **name**: nombre del sink y del fichero de salida.
- **paths**: rutas donde guardar los resultados.
- **format**: formato del fichero de salida.
- **saveMode**: modo de guardado.

Se debe crear un sink por cada source. Siempre debe existir un sink de error de validación. Las rutas de paths en el sink de error son las rutas de error de los sources con el mismo orden.

## 4. Catálogo

El catálogo de transformaciones y validaciones es una estructura de tipo clave-valor donde la clave es el nombre que recibe la función en los metadatos y el valor el código SQL asociado. Un ejemplo es:

```text
transformation_catalog = {
                            "notNull": "field1 is NOT NULL",
                            "notEmpty": "field1 != \"\"",
                            "current_timestamp": "CURRENT_TIMESTAMP"
                            "concat": CONCAT(field1, field2)
                        }
````

Si la funcionalidad hace uso de campos existentes en los datos se debe indicar con el marcador field< nº de campo diferente >. Adicionalmente si la función necesita otros caracteres se introducirán con el marcador addtional.