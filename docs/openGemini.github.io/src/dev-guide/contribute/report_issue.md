---
order: 1
---

# Report an Issue

Before you report an issue, please [search existing issues](https://github.com/openGemini/openGemini/issues) to check whether it's already been reported, or perhaps even fixed. If you choose to report an issue, please include the following in your report:

- Full details of your operating system (or distribution)--for example, `64bit Ubuntu 18.04`. To get your operating system details, run the following command in your terminal and copy-paste the output into your report:

  ```bash
  uname -srm
  ```

- How you installed openGemini. Did you use a pre-built package or did you build from source?

- The version of openGemini you're running. If you installed openGemini using a pre-built package, run the following command in your terminal and then copy-paste the output into your report:

  ```bash
  ts-server version # or ts-sql version / ts-store version / ts-meta version e.t.
  ```

There are many issue templates.

[New issue](https://github.com/openGemini/openGemini/issues/new/choose)

Answering these questions give the details about your problem so other contributors or openGemini users could pick up your issue more easily.

## Making good issues (TODO)

Except for a good title and detailed issue message, you can also add suitable labels to your issue via [/label](), especially which component the issue belongs to and which versions the issue affects. Many committers and contributors only focus on certain subsystems of openGemini. Setting the appropriate component is important for getting their attention.

Deep thoughts could help the issue proceed faster and help build your own reputation in the community.

## Understanding the issue's progress and status

Once your issue is created, other contributors might take part in. You need to discuss with them, provide more information they might want to know, address their comments to reach consensus and make the progress proceeds. But please realize there are always more pending issues than contributors are able to handle, and especially openGemini community is a global one, contributors reside all over the world and they might already be very busy with their own work and life. Please be patient! If your issue gets stale for some time, it's okay to ping other participants for more attention.

## Reporting security vulnerabilities

openGemini takes security and our users' trust very seriously. If you believe you have found a security issue in any of our open source projects, please responsibly disclose it by contacting [security@openGemini.org](mailto:security@openGemini.org). More details about security vulnerability reporting, including our GPG key, [can be found here](https://github.com/openGemini/openGemini/security/policy).
