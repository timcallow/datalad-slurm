#!/usr/bin/env bash

set -e

TMP="$HOME/tmp/benchmark/"

DIR="datalad_schedule_many_1_ouput"
mkdir $DIR
TMPDIR="$TMP/$DIR"
./test_08_timings_very_many_jobs_dont_finish.sh $TMPDIR
cp $TMPDIR/*/timing_*.txt $DIR/

DIR="datalad_schedule_many_4_ouputs"
mkdir $DIR
TMPDIR="$TMP/$DIR"
./test_08_timings_very_many_jobs_dont_finish.sh $TMPDIR 3
cp $TMPDIR/*/timing_*.txt $DIR/

DIR="datalad_schedule_many_7_ouputs"
mkdir $DIR
TMPDIR="$TMP/$DIR"
./test_08_timings_very_many_jobs_dont_finish.sh $TMPDIR 6
cp $TMPDIR/*/timing_*.txt $DIR/

# clean up

echo "clean up"

set +e

chmod -R u+w $TMP 
rm -Rf u+w $TMP

