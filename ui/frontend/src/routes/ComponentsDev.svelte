<script lang="ts">
  import { onDestroy } from 'svelte'
  import AreaChart from '../lib/components/AreaChart.svelte'
  import AgeHeat from '../lib/components/AgeHeat.svelte'
  import Button from '../lib/components/Button.svelte'
  import ConfirmDialog from '../lib/components/ConfirmDialog.svelte'
  import CopyButton from '../lib/components/CopyButton.svelte'
  import DataTable, { type Column } from '../lib/components/DataTable.svelte'
  import Duration from '../lib/components/Duration.svelte'
  import EmptyState from '../lib/components/EmptyState.svelte'
  import MetricCard from '../lib/components/MetricCard.svelte'
  import RelativeTime from '../lib/components/RelativeTime.svelte'
  import Sparkline from '../lib/components/Sparkline.svelte'
  import StatusBadge from '../lib/components/StatusBadge.svelte'
  import { toast } from '../lib/stores/toast.svelte'

  const statuses = ['pending', 'running', 'completed', 'failed', 'dead-lettered', 'cancelled', 'paused', 'waiting', 'retrying']
  const now = Date.now()
  interface Row {
    id: string
    status: string
    count: number
  }

  const columns: Column[] = [
    { key: 'id', label: 'ID' },
    { key: 'status', label: 'Status' },
    { key: 'count', label: 'Count', align: 'right', sortable: true },
  ]
  const rows = [
    { id: 'job-alpha', status: 'running', count: 12 },
    { id: 'job-beta', status: 'failed', count: 2 },
  ]
  let showDialog = $state(false)
  let liveMetricValue = $state(41)
  const liveMetricInterval = setInterval(() => {
    liveMetricValue += 1
  }, 1400)

  onDestroy(() => {
    clearInterval(liveMetricInterval)
  })
</script>

<section class="components-dev">
  <h2>BLACKBOX_COMPONENT_GALLERY</h2>

  <div class="panel">
    <h3>StatusBadge</h3>
    <div class="cluster">
      {#each statuses as status}
        <StatusBadge {status} class={status === 'paused' ? 'badge-paused' : ''} />
      {/each}
    </div>
  </div>

  <div class="grid">
    <MetricCard label="Failed" value={5} status="failed" dominant sub="danger dominant" href="#/jobs?status=failed" />
    <MetricCard label="Completed" value={128} sub="healthy" />
    <MetricCard label="Running" value={4} href="#/jobs?status=running" />
    <MetricCard label="Live delta" value={liveMetricValue} sub="delta-flash demo" />
  </div>

  <div class="panel">
    <h3>DataTable</h3>
    {#snippet cell(row: Row, column: Column)}
      {#if column.key === 'status'}
        <StatusBadge status={row.status} />
      {:else}
        {String((row as unknown as Record<string, string | number>)[column.key] ?? '')}
      {/if}
    {/snippet}
    <DataTable {columns} {rows} loading={false} rowKey={(row) => row.id} onRowClick={(row) => toast.push({ kind: 'info', msg: `row ${row.id}` })} {cell} />
    <DataTable {columns} rows={[]} loading={true} rowKey={(row) => row.id} {cell} />
    {#snippet empty()}
      <EmptyState title="Nothing scheduled" hint="Create a scheduled job to see it here." code="queue.Schedule()" />
    {/snippet}
    <DataTable {columns} rows={[]} loading={false} rowKey={(row) => row.id} emptyState={empty} {cell} />
  </div>

  <div class="panel">
    <h3>Actions</h3>
    <div class="cluster">
      <Button variant="primary" icon="retry">Retry</Button>
      <Button variant="secondary">Secondary</Button>
      <Button variant="ghost">Ghost</Button>
      <Button variant="destructive">Delete</Button>
      <Button variant="secondary" loading>Loading</Button>
      <CopyButton text="job-alpha" />
      <Button variant="secondary" onclick={() => toast.push({ kind: 'info', msg: 'sample toast' })}>Toast</Button>
      <Button variant="destructive" onclick={() => { showDialog = true }}>Confirm</Button>
    </div>
  </div>

  <div class="panel">
    <h3>Time</h3>
    <div class="cluster">
      <RelativeTime ts={now - 245000} mode="rel" live />
      <RelativeTime ts={now - 245000} mode="both" />
      <RelativeTime mode="clock" live />
      <Duration from={now - 65000} />
      <AgeHeat ts={now - 30000} />
      <AgeHeat ts={now - 180000} />
      <AgeHeat ts={now - 700000} />
    </div>
  </div>

  <div class="panel">
    <h3>Charts</h3>
    <Sparkline data={[1, 4, 2, 8, 5, 9]} />
    <AreaChart completed={[2, 5, 9, 8, 13]} failed={[0, 1, 0, 2, 1]} period="24h" />
    <AreaChart completed={[]} failed={[]} period="1h" />
    <AreaChart completed={[5]} failed={[1]} period="7d" />
    <AreaChart completed={[1, 1000, 2500000]} failed={[0, 5000, 3]} period="7d" />
  </div>

  <EmptyState title="Empty state" hint="The shared empty component carries the .empty hook." code="queue.Enqueue()" />
</section>

{#if showDialog}
  <ConfirmDialog
    title="Delete sample"
    body="This shows the typed confirmation path."
    blastRadius="permanently deletes the sample row"
    confirmWord="DELETE"
    confirmLabel="Delete"
    onCancel={() => { showDialog = false }}
    onConfirm={() => { showDialog = false; toast.push({ kind: 'ok', msg: 'confirmed' }) }}
  />
{/if}

<style>
  .components-dev {
    display: grid;
    gap: var(--sp-6);
  }

  h2 {
    font-size: var(--fs-title);
  }

  h3 {
    margin-bottom: var(--sp-3);
    color: var(--fg-secondary);
    font-size: var(--fs-label);
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }

  .panel {
    padding: var(--sp-4);
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .cluster {
    display: flex;
    flex-wrap: wrap;
    gap: var(--sp-2);
    align-items: center;
  }

  .grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: var(--sp-3);
  }

  @media (max-width: 1024px) {
    .grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
  }

  @media (max-width: 767px) {
    .grid {
      grid-template-columns: 1fr;
    }
  }
</style>
