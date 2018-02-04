## Ansible playbook with vagrant on virtualbox for kafka-cluster.


####  Provisioning Stack

  1.  VirtualBox  - OpenSource Hypervisior from Oracle
  2.  Vagrant     - Orchestration
  3.  Ansible     - Configuration Management tool

####  Software configured during provisioning

  1. Java 8u131
  2. Zookeeper 3.4.10
  3. Kafka  1.0

## Installation

1. Download VirtualBox on Mac OSX

    https://download.virtualbox.org/virtualbox/5.2.6/VirtualBox-5.2.6-120293-OSX.dmg

2. Download Vagrant on Mac OSX

    https://releases.hashicorp.com/vagrant/2.0.2/vagrant_2.0.2_x86_64.dmg?_ga=2.224755610.1496815869.1517589623-203266063.1517589623

3. Download Ansible on Mac OSX
      - Brew Install
        - `brew install ansible`

     OR

      - Native Python Install
        - `sudo easy_install pip`
        - `sudo pip install ansible --quiet`

4. Provision Dev Environment
      - Change to infra directory
        - `cd infra`
      - Start the vagrant environment
        - `vagrant up`

      - In case command failed due to timeout
        - `vagrant up --provision`

5. Once it got successfully provisioned
      - login into kafka-cluster
        - `vagrant ssh kafka-cluster`
