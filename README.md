# About
RdfUploadService is a Webservice which accepts POST requests with RDF payload. It works with various RDF formats. The RDF format needs to be defined in the Content-Type Header.

A sample implementation can be found in the [Nanopubs project](https://github.com/tkuhn/nanopub-server/blob/e1355e9e3dca06f41f322cf1f7b498309cd2930f/src/main/java/ch/tkuhn/nanopub/server/NanopubDb.java#L228-L231).

# Docker
## Build
```
docker build -t rdf-upload-service .
```
## Usage
```
docker run rdf-upload-service
```
## Run
Environment variables in squared brackets are optional. Username will only be used in combination with a password
```
docker run \
  -it --rm \
  -p 80:80 \
  -e ENDPOINT=<your sparql endpoint> \
  [-e UPDATE_ENDPOINT=<optional if you have an additional endpoint for updates> \]
  [-e USERNAME=<username for authentication against ENDPOINT and/or UPDATE_ENDPOINT> \
   -e PASSWORD=<password for authentication against ENDPOINT and/or UPDATE_ENDPOINT> \]
  rdf-upload-service 
```
