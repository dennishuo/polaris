
#!/usr/bin/env bash

# eval $(grep -aEo 'realm: [^:]*:[^:]*:[^:]*' /tmp/polaris.log | perl -pe 's/realm: [^:]*: ([^:]*):([^:]*)/export CLIENT_ID=$1 CLIENT_SECRET=$2/')
export CLIENT_ID=root CLIENT_SECRET=s3cr3t

echo export CLIENT_ID=$CLIENT_ID
echo export CLIENT_SECRET=$CLIENT_SECRET
echo export TOKEN=$(
  curl \
    -s \
    -X POST \
    "http://${POLARIS_HOST:-localhost}:8181/api/catalog/v1/oauth/tokens" \
    -d "grant_type=client_credentials" \
    -d "client_id=$CLIENT_ID" \
    -d "client_secret=$CLIENT_SECRET" \
    -d "scope=PRINCIPAL_ROLE:ALL" |
    jq -r .access_token
)
