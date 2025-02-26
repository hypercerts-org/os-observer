# gce-dev-vm

Sets up a GCE vm suitable for remote development.

# usage

> [!WARNING]
> Make sure you are logged in to the correct GCP project before running the
> following commands.

Create a `terraform.tfvars` file with the following content. To pick your zone
and region, the rule of thumb is to pick the region closest to you. Use this
[link](https://googlecloudplatform.github.io/region-picker/) to find the region
and zone closest to you.

```hcl
project_id       = "your-project-id"
credentials_file = "/path/to/application_default_credentials.json"
public_key_path  = "/path/to/ssh_key.pub"
ssh_user         = "user"
zone             = "your-zone1"
region           = "your-zone1-a"
```

Now, initialize the terraform module, and plan the changes.

```bash
terraform init
terraform plan
```

Check that everything looks good, and apply the changes.

```bash
terraform apply
```

After the apply is complete, you should see the external IP of the VM in the
output. You can now ssh into the VM using the following command.

```bash
ssh -i /path/to/ssh_key user@external-ip
```

# features

Installs development tools, `python3.12` from source and `nvm` for managing
node.

## post-install

After the VM is created, you should change the password for the user. You can
run the following command to change the password.

```bash
sudo passwd user
```

You should also add ssh keys to the newly created VM. You can either copy your
current ssh keys to the VM or generate new ones. To copy your current ssh keys
to the VM, you can run the following command.

```bash
ssh-copy-id -i /path/to/ssh_key user@external-ip
```

To generate new ssh keys, you can run the following command.

```bash
ssh-keygen -t rsa -b 4096 -C "user@email.com"
```

After generating the keys, follow
[GitHub's guide](https://docs.github.com/en/github/authenticating-to-github/adding-a-new-ssh-key-to-your-github-account)
to add the ssh key to your GitHub account.

You should also set `GPG` keys for signing commits. You can follow
[GitHub's guide](https://docs.github.com/en/github/authenticating-to-github/managing-commit-signature-verification)
as well.

## convenience scripts

We also provide [`scrips/connect.sh`](./scripts/connect.sh), which you can use
to automatically add the machine `ip` to your `~/.ssh/config` file. This will
allow you to ssh into the machine using the `ssh user@name:project:region`
command.

With this script, we can create two aliases in our `~/.bashrc` file, to start
and stop the VM. These will add and remove the machine `ip` from the
`~/.ssh/config` file respectively, as well as start and stop the VM, to save
costs.

```bash
alias osostart="bash /path/to/gce-dev-vm/scripts/connect.sh start --project $PROJECT --instance $NAME -z $ZONE -u $USER"
alias osostop="bash /path/to/gce-dev-vm/scripts/connect.sh stop --project $PROJECT --instance $NAME --zone $ZONE"
```
