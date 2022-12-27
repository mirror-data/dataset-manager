# Mirror Dataset Manager

## API

### POST /upload
to upload a csv file and convert it to a sqlite database.

### GET /list
to list all the databases.

### POST /{UUID}/exec
>  request json body: sql

to execute a query on a database.

### GET /{UUID}/schema
to get the schema of a database.
