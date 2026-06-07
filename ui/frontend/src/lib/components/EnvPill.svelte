<script lang="ts">
  type Env = 'prod' | 'staging' | 'dev'
  let { env }: { env: Env } = $props()
</script>

<span class="env-pill env-{env}">{env}</span>

<style>
  .env-pill {
    display: inline-flex;
    align-items: center;
    height: 20px;
    padding: 0 var(--sp-2);
    border: var(--border);
    border-radius: var(--radius-chip);
    font-family: var(--font-mono);
    font-size: var(--fs-micro);
    font-weight: var(--fw-label);
    font-feature-settings: var(--num);
    letter-spacing: 0.06em;
    line-height: 1;
    text-transform: uppercase;
    color: var(--fg-secondary);
    background: var(--bg-sunken);
  }

  .env-prod {
    color: var(--sig-danger);
    border-color: var(--sig-danger);
    background: var(--sig-danger-bg);
  }

  .env-staging {
    color: var(--sig-warn);
    border-color: var(--sig-warn);
    background: var(--sig-warn-bg);
  }

  .env-dev {
    color: var(--sig-neutral);
    border-color: var(--sig-neutral);
    background: var(--sig-neutral-bg);
  }
</style>
