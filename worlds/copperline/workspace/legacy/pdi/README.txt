nightly_load - Copperline nightly load
Pentaho Data Integration 8.3.0.0-371
A. Mireles, last touched 2021-09-30


One job file, called once per job name. The scheduler passes JOB_NAME and
BUSINESS_DATE and nothing else. set_constants reads the site settings,
resolve_load_control reads the control row for that job and that night and
exports the load context, the four load steps consume it, history_complete
closes the bracket. The mark a load step cuts from is the mark the previous
night reached, which is why resolve_load_control joins the control table back a
day, and why the control table is read once and only in that step.

Site settings live in kettle.properties on the PDI host rather than in these
files, so the same job can be pointed at a copy of the warehouse without editing
anything. The database password is encrypted with the repository key and will
not decrypt off that host. To run a night by hand use kitchen.sh; the AutoSys
definitions carry the exact arguments, including the date format.


TODO 2021-06-02 AM: supplier_master arrives from the Windows box on a share
mounted at CPL_IFACE_IN. When the share is not there the CSV step reads nothing
and the job still finishes green. Worth a file check in front of it. Low
priority, it has happened twice in two years.
