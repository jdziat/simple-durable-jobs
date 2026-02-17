<script lang="ts">
  let { title, value, color = '#333', href = '' }: { title: string; value: number; color?: string; href?: string } = $props()
</script>

{#if href}
  <a class="card clickable" style="--accent-color: {color}" {href}>
    <div class="value">{value.toLocaleString()}</div>
    <div class="title">{title}</div>
  </a>
{:else}
  <div class="card" style="--accent-color: {color}">
    <div class="value">{value.toLocaleString()}</div>
    <div class="title">{title}</div>
  </div>
{/if}

<style>
  .card {
    background: white;
    border-radius: 8px;
    padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    border-left: 4px solid var(--accent-color);
    display: block;
    text-decoration: none;
    color: inherit;
  }

  .card.clickable {
    cursor: pointer;
    transition: box-shadow 0.2s, transform 0.2s;
  }

  .card.clickable:hover {
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    transform: translateY(-1px);
  }

  .value {
    font-size: 32px;
    font-weight: 700;
    color: var(--accent-color);
  }

  .title {
    font-size: 14px;
    color: #666;
    margin-top: 4px;
  }
</style>
