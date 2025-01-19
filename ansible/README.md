# IaaC configuration for HDFS + Spark cluster
This directory contains scripts for setting up the infrastructure:
- Apache Spark standalone cluster of single master node and a fleet of worker nodes,
- HDFS cluster (`namenode` on master node, `datanodes` on workers) from Apache Hadoop.

## Requirements
- Ansible binary installed. See the [project homepage](https://docs.ansible.com/ansible/latest/index.html).
- Set up OS and SSH configuration on target nodes.
- Created passwordless SSH keys in:
  - _roles/hdfs/templates/hp-hdfs.ssh.private_ 
  - _roles/hdfs/templates/hp-hdfs.ssh.public_
  - _roles/spark/templates/hp-spark.ssh.private_
  - _roles/spark/templates/hp-spark.ssh.public_
- Correctly set up _inventory_ file with master and worker nodes.

## CLI commands
All commands refer to _inventory_ file as the list of target nodes.
- Test connection to the target nodes:
  - `ansible -i inventory -m ping all` 
- Gather facts from the target nodes:
  - `ansible -i inventory -m setup all`
- Apply whole IaaC (HDFS, Spark and utilities):
  - `ansible-playbook -i inventory benchmark-cluster.yaml`
- Apply only partial IaaC (available tags: _hdfs-only_, _spark-only_):
  - `ansible-playbook -i inventory benchmark-cluster.yaml --tags <<tag>>` 

## Configuration details
Please note that the cluster uses up-to-date version of the Java Development Kit (JDK 21) instead of "recommended"
ancient version of JDK 8. This imposes a few limitations originating from the libraries used (mostly: Hadoop).

### HDFS
- HDFS file browser from the WebUI does not work due to its incompatibility with Java 11.
- Replication on the HDFS cluster is set to 1, as it is a benchmarking installation, not a production one.
- Memory locked limit on datanodes is set to ~ 4GB to not favour HDFS' workers over Spark's ones.

### Spark
- Spark worker's memory is set to 24GB.