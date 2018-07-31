#!/usr/bin/env bash

if [ -z "$XAP_OPEN_SHA" ]
then
  SHA=`git rev-parse HEAD`
else
  SHA="$XAP_OPEN_SHA"
fi

echo [ \"xap-open\":\"${SHA}\" ] > xap-open-metadata.txt
