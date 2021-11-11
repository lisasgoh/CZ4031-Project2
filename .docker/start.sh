#!/bin/bash
while ! nc -z tpch-db 5433; do sleep 3; done
python project.py --host=0.0.0.0