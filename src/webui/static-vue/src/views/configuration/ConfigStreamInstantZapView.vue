<!--
  SPDX-License-Identifier: GPL-3.0-or-later
  Copyright (C) 2026 Tvheadend contributors
-->
<script setup lang="ts">
/*
 * Configuration → Stream → Instant Zapping.
 *
 * Singleton config form for the `dvbbuffer` idnode
 * (`src/dvbbuffer/conf.c`, ACCESS_ADMIN), the settings of instant
 * zapping (ring buffer, warm muxes, live start ...). Mirrors
 * the legacy ExtJS page at `static/app/dvbbuffer.js`, which uses
 * `idnode_simple` against `api/dvbbuffer/config`.
 *
 * The muxes kept warm are chosen per mux (Configuration → DVB
 * Inputs → Muxes, "Prebuffer"), the live start per stream profile;
 * both are plain idnode fields of those classes. Capability gate
 * handled at the router (`configStreamInstantZapGuard`) + the L3
 * tab strip (`ConfigStreamLayout`).
 */
import IdnodeConfigForm from '@/components/IdnodeConfigForm.vue'
</script>

<template>
  <IdnodeConfigForm
    load-endpoint="dvbbuffer/config/load"
    help-page="class/dvbbuffer"
    save-endpoint="dvbbuffer/config/save"
  />
</template>
