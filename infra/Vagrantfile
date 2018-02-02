# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.box = "centos/7"
  config.vm.box_check_update = false
  config.ssh.insert_key = false
  config.vm.synced_folder "../", "/opt/kafka_workshop", :nfs => { :mount_options => ["dmode=777","fmode=777"] }

  config.vm.network "forwarded_port", guest: 2181, host: 2181
  config.vm.network "forwarded_port", guest: 9092, host: 9092
  config.vm.network "forwarded_port", guest: 9092, host: 9093

  config.vm.provider "virtualbox" do |vb|
    vb.memory = 3048
    vb.cpus = 2
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize ["modifyvm", :id, "--ioapic", "on"]
    vb.customize ["modifyvm", :id, "--nictype1", "virtio"]
  end

  config.vm.hostname = "dev"
  config.vm.network :private_network, ip: "192.168.33.55"

  config.vm.define :dev do |dev|
  end

  config.vm.provision "ansible" do |ansible|
      ansible.playbook = "provision.yml"
      ansible.inventory_path = "inventory"
      ansible.sudo = true
      ansible.verbose = true
  end

end
