#!/usr/bin/env bash
#run the twitter connector
connect-standalone connect-standalone.properties twitter.properties
#or (linux / mac osx)
connect-standalone.sh connect-standalone.properties twitter.properties
#or (windows)
connect-standalone.bat connect-standalone.properties twitter.properties