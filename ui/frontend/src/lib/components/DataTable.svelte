<script module lang="ts">
  export interface Column {
    key: string
    label: string
    align?: 'left' | 'right' | 'center'
    width?: string
    sortable?: boolean
  }
</script>

<script lang="ts" generics="T">
  import type { Snippet } from 'svelte'
  import EmptyState from './EmptyState.svelte'

  let {
    columns,
    rows,
    loading = false,
    rowKey,
    onRowClick,
    sort,
    onSort,
    emptyState,
    class: className = '',
    cell,
  }: {
    columns: Column[]
    rows: T[]
    loading?: boolean
    rowKey: (row: T) => string
    onRowClick?: (row: T) => void
    sort?: { key: string; dir: 'asc' | 'desc' }
    onSort?: (key: string) => void
    emptyState?: Snippet
    class?: string
    cell?: Snippet<[T, Column]>
  } = $props()

  let skeletonRows = $derived(Math.max(rows.length, 5))

  function activate(event: KeyboardEvent, row: T) {
    if (!onRowClick) return
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault()
      onRowClick(row)
    }
  }
</script>

{#if !loading && rows.length === 0}
  {#if emptyState}
    {@render emptyState()}
  {:else}
    <EmptyState title="No rows" hint="No records match the current query." />
  {/if}
{:else}
  <div class="table-scroll">
    <table class={`data-table ${className}`}>
      <thead>
        <tr>
          {#each columns as column}
            <th
              class:right={column.align === 'right'}
              class:center={column.align === 'center'}
              style:width={column.width}
              aria-sort={sort?.key === column.key ? (sort.dir === 'asc' ? 'ascending' : 'descending') : undefined}
            >
              {#if column.sortable}
                <button type="button" onclick={() => onSort?.(column.key)}>
                  <span>{column.label}</span>
                  <span class="sort-caret" aria-hidden="true">{sort?.key === column.key ? (sort.dir === 'asc' ? '↑' : '↓') : '↕'}</span>
                </button>
              {:else}
                {column.label}
              {/if}
            </th>
          {/each}
        </tr>
      </thead>
      <tbody>
        {#if loading}
          {#each Array.from({ length: skeletonRows }) as _, rowIndex}
            <tr class="skeleton-row">
              {#each columns as column, columnIndex}
                <td class:right={column.align === 'right'} class:center={column.align === 'center'}>
                  <span class="skeleton" style={`width:${columnIndex === 0 ? '56%' : rowIndex % 2 === 0 ? '72%' : '42%'}`}></span>
                </td>
              {/each}
            </tr>
          {/each}
        {:else}
          {#each rows as row (rowKey(row))}
            <tr
              class:clickable={!!onRowClick}
              role={onRowClick ? 'button' : undefined}
              tabindex={onRowClick ? 0 : undefined}
              onclick={() => onRowClick?.(row)}
              onkeydown={(event) => activate(event, row)}
            >
              {#each columns as column}
                <td class:right={column.align === 'right'} class:center={column.align === 'center'}>
                  {#if cell}
                    {@render cell(row, column)}
                  {:else}
                    {String((row as Record<string, unknown>)[column.key] ?? '')}
                  {/if}
                </td>
              {/each}
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>
{/if}

<style>
  .table-scroll {
    overflow: auto;
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .data-table {
    width: 100%;
    border-collapse: collapse;
    color: var(--fg-primary);
    font-size: var(--fs-body);
  }

  thead {
    position: sticky;
    top: 0;
    z-index: 1;
    background: var(--bg-raised);
  }

  th,
  td {
    height: var(--row-h);
    padding: 0 var(--sp-3);
    border-bottom: var(--border);
    text-align: left;
    vertical-align: middle;
  }

  th {
    color: var(--fg-secondary);
    font-size: var(--fs-micro);
    font-weight: var(--fw-label);
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  tbody tr:last-child td {
    border-bottom: 0;
  }

  .right {
    text-align: right;
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
  }

  .center {
    text-align: center;
  }

  th button {
    display: inline-flex;
    align-items: center;
    gap: var(--sp-1);
    border: 0;
    background: transparent;
    color: inherit;
    font: inherit;
    letter-spacing: inherit;
    text-transform: inherit;
    cursor: pointer;
  }

  .sort-caret {
    color: var(--fg-secondary);
    font-family: var(--font-mono);
  }

  .clickable {
    cursor: pointer;
  }

  .clickable:hover,
  .clickable:focus-visible {
    background: var(--bg-sunken);
  }

  .skeleton {
    display: inline-block;
    height: 10px;
    border-radius: var(--radius-chip);
    background: linear-gradient(90deg, var(--bg-sunken), var(--hairline), var(--bg-sunken));
    animation: shimmer var(--dur-sweep) var(--ease) infinite alternate;
  }

  @keyframes shimmer {
    from { opacity: 0.45; }
    to { opacity: 1; }
  }
</style>
