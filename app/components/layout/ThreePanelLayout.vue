<template>
  <div class="three-panel-grid">
    <aside class="panel-left">
      <slot name="left" />
    </aside>
    <main class="panel-middle">
      <slot name="middle" />
    </main>
    <section class="panel-right">
      <slot name="right" />
    </section>
  </div>
</template>

<style scoped>
.three-panel-grid {
  display: grid;
  grid-template-columns: minmax(16rem, 22rem) minmax(24rem, 1fr) minmax(24rem, 1fr);
  grid-template-rows: 1fr;
  height: 100%;
  width: 100%;
  overflow: hidden;
}

.panel-left,
.panel-middle,
.panel-right {
  overflow: hidden;
  height: 100%;
}
</style>
