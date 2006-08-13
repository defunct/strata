#!/bin/sh

dir=`dirname $0`

groovy -cp $dir/mix/import/xalan.jar $dir/mix/import/build.groovy "$@"
