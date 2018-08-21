This script monitors multiple Aerospike metrics, represented as a single Nagios service
(not to be confused with [aerospike_nagios.py](https://github.com/aerospike/aerospike-nagios),
which monitors only one metric per Nagios service).

# Requirements

- Python 2
- `asinfo` tool

# Usage

The script should be installed on each node in Aerospike cluster. Sample usage
(a line from Nagios NRPE config file):

    command[check_aerospike]=/usr/lib/nagios/plugins/aerospike_nagios2.py --cluster-size 3 

Use `-h` option to see all command line switches and defaults.
