## Ansible playbook with vagrant on virtualbox for dev environment.


####  Provisioning Stack

  1.  VirtualBox  - OpenSource Hypervisior from Oracle
  2.  Vagrant     - Orchestration
  3.  Ansible     - Configuration Management tool
  4.  nfs         -  filesystem sharing

####  Software configured during provisioning

  1. Java 8u131
  2. NPM & Nodejs
  3. Zookeeper 3.4.10
  4. Kafka  2.10_0.10.2.0
  5. git
  6. postgres 9.6

## [Installation]

#1. Download VirtualBox on Mac OSX

    http://download.virtualbox.org/virtualbox/5.1.26/VirtualBox-5.1.26-117224-OSX.dmg

#2. Download Vagrant on Mac OSX

    https://releases.hashicorp.com/vagrant/2.0.0/vagrant_2.0.0_x86_64.dmg?_ga=2.212131473.405336357.1505108089-1065560910.1504848368

#3. Download Ansible on Mac OSX

      - Brew Install
      > brew install ansible

     OR

      - Native Python Install
      > sudo easy_install pip
      > sudo pip install ansible --quiet

#4. Git checkout infra repository for qapmtw dev environment

       git clone git@gitlab.qapm.internal:qapm/infra.git

   **Note** - Clone `infra` and all the other components inside the same parent directory

#5. Provision Dev Environment

       > cd infra

       - run the command
       > vagrant up
       - Provide your admin system password required for nfs mount of current direc-tory for code sharing b/w local and vagrant machine

       - In case command failed due to timeout
       > vagrant up --provision

#6. Once it got successfully provisioned

     - login into dev environment
     >  vagrant ssh dev
