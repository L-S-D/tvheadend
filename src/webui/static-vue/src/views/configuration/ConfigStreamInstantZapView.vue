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
 * The muxes kept warm are chosen here ("Warm mux 1..n", one single
 * select per warm mux, as many as "Maximum warm muxes") or per
 * mux (Configuration → DVB Inputs → Muxes, "Prebuffer"): both are
 * the same mux flag. The live start is chosen per stream profile. Capability gate
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
