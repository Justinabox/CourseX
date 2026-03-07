# CourseX

**[coursex.school.rip](https://coursex.school.rip)**

A modern reimagining of USC's [Schedule of Classes](https://classes.usc.edu), built by students, for students. CourseX puts the browsing experience front and center — fast search, rich course details, enrollment status, instructor info, and syllabus access, all in one clean interface.

All contributions are welcome.

---

## Features

- **Three-panel layout** — browse programs and terms, scan the course list, and view full section details all at the same time
- **Fast course search & filtering** — search by keyword, filter by GE requirement, units, days, and more with a virtualized list that handles hundreds of courses without lag
- **Instructor RMP** — see instructors' RateMyProfessors ratings
- **Schedule calendar** — visual weekly schedule preview for any selected section
- **Syllabus links** — direct links to cached syllabi when available, no more simple syllabus headache
- **Google OAuth login** — sign in to view classes, as per current USC requirement ;)
---

## Tech Stack

| Layer | Technology |
|---|---|
| Framework | [Nuxt 4](https://nuxt.com) + [Vue 3](https://vuejs.org) |
| Styling | [Tailwind CSS v4](https://tailwindcss.com) + [Nuxt UI v3](https://ui.nuxt.com) |
| State | [Pinia](https://pinia.vuejs.org) |
| Database | [Drizzle ORM](https://orm.drizzle.team) + [Neon](https://neon.tech) (PostgreSQL) |
| Auth | [nuxt-auth-utils](https://github.com/atinux/nuxt-auth-utils) (Google OAuth) |
| Virtualization | [TanStack Virtual](https://tanstack.com/virtual) |
| Runtime | [Bun](https://bun.sh) |
| Deployment | Cloudflare Workers |

---

## Getting Started

### Prerequisites

- [Bun](https://bun.sh) installed (`curl -fsSL https://bun.sh/install | bash`)
- A [Neon](https://neon.tech) PostgreSQL database
- Google OAuth credentials ([Google Cloud Console](https://console.cloud.google.com))

### 1. Clone the repository

```bash
git clone https://github.com/MeloticZ/CourseX
cd CourseX
```

### 2. Install dependencies

```bash
bun install
```

### 3. Configure environment variables

Create a `.env` file in the project root:

```env
DATABASE_URL=postgresql://<user>:<password>@<host>/<db>?sslmode=require
NUXT_OAUTH_GOOGLE_CLIENT_ID=your_google_client_id
NUXT_OAUTH_GOOGLE_CLIENT_SECRET=your_google_client_secret
NUXT_SESSION_PASSWORD=a_random_32_char_secret
NUXT_PUBLIC_SYLLABUS_DOMAIN=https://your-syllabus-bucket-url
```

### 4. Push the database schema

```bash
bunx drizzle-kit push
```

### 5. Start the development server

```bash
bun run dev
```

The app will be available at `http://localhost:3000`.

---

## Contributing

Contributions, bug reports, and feature requests are all welcome. Please open an issue or submit a pull request.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m 'Add my feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request
