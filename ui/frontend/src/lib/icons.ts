export const icons = {
  dashboard: 'M3 13h8V3H3v10Zm10 8h8V3h-8v18ZM3 21h8v-6H3v6Zm2-16h4v6H5V5Zm10 0h4v16h-4V5ZM5 17h4v2H5v-2Z',
  jobs: 'M6 3h9l5 5v16H6V3Zm8 2v5h5M9 14h8M9 18h8M9 22h5',
  queue: 'M4 7h16v4H4V7Zm0 6h16v4H4v-4Zm0 6h16v4H4v-4ZM7 9h.01M7 15h.01M7 21h.01',
  schedule: 'M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18Zm0 4v5l3 2',
  workflow: 'M6 6h6v6H6V6Zm12 0h6v6h-6V6ZM6 18h6v6H6v-6Zm6-9h6M9 12v6M21 12v4a2 2 0 0 1-2 2h-7',
  retry: 'M21 12a8 8 0 1 1-2.34-5.66L21 8M21 3v5h-5',
  pause: 'M8 5h4v18H8V5Zm8 0h4v18h-4V5Z',
  resume: 'M7 5v18l15-9L7 5Z',
  cancel: 'M6 6l18 18M24 6 6 24',
  delete: 'M7 7h18M10 7V5h12v2M11 11v14h11V11M15 14v7M19 14v7',
  purge: 'M5 8h22M9 8l2 18h12l2-18M13 4h8l1 4M12 14h12M14 18h8',
  copy: 'M9 9h14v14H9V9Zm-4 8H3V3h14v2M7 5h10v2',
  sun: 'M12 5V2m0 22v-3M5.64 5.64 3.51 3.51m16.98 16.98-2.13-2.13M5 12H2m22 0h-3M5.64 18.36l-2.13 2.13M20.49 3.51l-2.13 2.13M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8Z',
  moon: 'M21 14.5A8.5 8.5 0 0 1 10.5 4a7 7 0 1 0 10.5 10.5Z',
  chevron: 'm9 6 6 6-6 6',
  dot: 'M12 8a4 4 0 1 0 0.01 0Z',
  search: 'M11 5a6 6 0 1 0 0 12 6 6 0 0 0 0-12Zm4.5 10.5L21 21',
  command: 'M9 8a3 3 0 1 0-3 3h3V8Zm0 3v6a3 3 0 1 1-3-3h3Zm6-3v3h3a3 3 0 1 0-3-3Zm0 3v3h3a3 3 0 1 1-3 3v-6ZM9 11h6v6H9v-6Z',
} as const

export type IconName = keyof typeof icons
