---
title: Bounty Program
sidebar_position: 2
---

:::info
The aim of our bounty program is to encourage and reward the community for contributing to the growth and experimentation on OSO. Bounties are only available to members of the [Kariba Data Collective](https://kariba.network).
:::

## Competitive Bounties

Occassionally we post data science bounties and run community competitions. Each bounty has a set of requirements and a reward. Generally, the first person to meet the requirements will receive the reward although in some cases there may be multiple prizes or an award for runner-ups.

Some of our past data science bounties can be found on [Bountycaster](https://www.bountycaster.xyz/fid/19412).

The best way to learn about new data bounties is to follow us on [Twitter](https://twitter.com/OSObserver) or join our [Discord](https://www.opensource.observer/discord). You can also see a list of any active bounties from our GitHub issues [here](https://github.com/opensource-observer/insights/issues).

## Ongoing Bounties

We also have a number of ongoing bounties for contributing to the OSS Directory. These bounties are available to all members of the Data Collective and are paid out in USDC.

:::note
Please reach out to one of the maintainers for guidance on what projects and ecosystems we're currently looking for help with. We don't want to waste your time on projects that are already well-covered or that we don't have the capacity to review.
:::

### Adding a New Project

> 💰 1.00 USDC per new project successfully added to the OSS Directory.

The new entry must include a project slug (unique identifier), name, and an initial GitHub organization or repository.

Example:

```
version: 3
slug: my-project
name: My Project
github:
  - url: https://github.com/myproject
```

Any additional artifacts beyond the initial GitHub organization or repo will be awarded per the schedule below.

### Assigning Artifacts to Projects

We will award different amounts depending on the type of artifact and the type of update. Updates usually take the form of additions, but we also accept modifications (eg, renaming or correcting information about an artifact) and deletions.

The following is the current list of available bounties:

#### URL of a GitHub organization

> 💰 0.50 USDC (up to a max of 2.00 USDC per project).

Example:

```
github:
  - url: https://github.com/myproject
  - url: https://github.com/myrelatedproject # addition
```

#### URL of a GitHub repository

> 💰 0.10 USDC (up to a max of 1.00 USDC per project).

Note: the repo cannot already be contained in the project’s GitHub organization.

Example:

```
github:
  - url: https://github.com/myproject
  - url: https://github.com/hackathon-org/myproject-hackathon # addition
```

#### URL of an NPM package

> 💰 0.10 USDC (up to a max of 1.00 USDC per project).

Example:

```
npm:
  - url: https://www.npmjs.com/package/myproject-sdk # addition
```

#### Blockchain wallet address

> 💰 0.50 USDC per address (up to a max of 5.00 USDC per project).

The entry must include the network(s) and the `wallet` tag; it should also tag whether the wallet is an `EOA` or a `safe`. Where possible, a name for the address is appreciated.

Example:

```
blockchain:
  - address: "0x1234567890ABCDEFFEDCBA098765432123456789" # addition
    tags:
      - wallet
      - safe
    networks:
      - mainnet
    name:
      - Primary Multisig
```

#### Deployer address

> 💰 0.50 USDC per address (up to a max of 5.00 USDC per project).

The entry must include the network(s) and the tags `EOA` and `deployer`.

Example:

```
blockchain:
  - address: "0x1234567890ABCDEFFEDCBA098765432123456789" # addition
    tags:
      - EOA
      - deployer
    networks:
      - mainnet
```

#### Contract or factory address

> 💰 0.10 USDC per address (up to a max of 5.00 USDC per project).

The entry must include the network and that tags `contract` or `factory`. Where possible, a name for the contract or factory is appreciated.

Example:

```
blockchain:
  - address: "0x1234567890ABCDEFFEDCBA098765432123456789" # addition
    tags:
      - contract
    networks:
      - mainnet
    name:
      - MyContract
```

#### Network tag for a blockchain address

> 💰 0.05 USDC per address (up to a max of 1.00 USDC per project).

This bounty is only awarded for addresses that already have at least one network tag and the update includes an additional network with transaction activity on it.

Example:

```
blockchain:
  - address: "0x1234567890ABCDEFFEDCBA098765432123456789"
    tags:
      - EOA
      - wallet
    networks:
      - mainnet
      - new-network # additional network
```

### Reviewing Contributions

All contributions will be reviewed by the maintainers of the OSS Directory. We will do our best to review contributions in a timely manner, but please be patient with us. We will reach out to you via GitHub if we have any questions or concerns about your contribution.

When you submit the PR, please provide an estimate of the total bounty amount you expect to receive. We will use this as a guide when reviewing your contribution.
