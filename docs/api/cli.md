<style>
.md-typeset h2, h3, h4 {
  font-weight: 400;
  font-family: var(--md-code-font-family);
}

.md-typeset h2 {
  border-bottom-style: solid;
  border-color: var(--md-default-fg-color--lighter);
  border-width: 2px;
}

.md-typeset h3, h4 {
  border-bottom-style: dashed;
  border-color: var(--md-default-fg-color--lighter);
  border-width: 1px;
}
</style>

# CLI Reference

This page provides documentation for our command line tools.

::: mkdocs-click
    :module: proxystore.globus.cli
    :command: cli
    :prog_name: proxystore-globus-auth
    :depth: 1
    :list_subcommands: True
    :style: table

::: mkdocs-click
    :module: proxystore.endpoint.cli
    :command: cli
    :prog_name: proxystore-endpoint
    :depth: 1
    :list_subcommands: True
    :style: table

::: mkdocs-click
    :module: proxystore.p2p.relay.run
    :command: cli
    :prog_name: proxystore-relay
    :depth: 1
    :list_subcommands: True
    :style: table
