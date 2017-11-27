# HTCondor for MCNP simulations

This linux command line tool will help you send simulations on a HTCondor cluster, that has a common directory containing MCNP code.

## INSTALL

Clone this repository and add the bin directory to the PATH variable in your bashprofile.

## USAGE

```
usage: condor_mcnp [-h] [-k] [-s] [-m] PATH_TO_MCNP INPUT CORE NPS

Create multiple jobs to launch on an HTCondor infrastructure

positional arguments:
  PATH_TO_MCNP          Path to MCNP directory to set env variables for
                        execution.
  INPUT                 MCNP intput file.
  CORE                  Number of cores needed for the simulation.
  NPS                   Nuber of particles on each core.

optional arguments:
  -h, --help            show this help message and exit
  -k, --KCODE           This parameter is needed to activate KCODE
                        calculations -- STILL NOT ACTIVE.
  -s, --HTCondor_submit
                        Launch HTCondor batch-system after splitting files.
  -m, --HTCondor_merge  Once the simulation has finished, you may use this
                        parameter to create a merget MCTAL.
```
