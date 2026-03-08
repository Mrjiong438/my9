# 仓库指南（My9）

本指南面向贡献者与自动化代理，目标是与当前代码库实践保持一致。

## 项目结构与模块组织
- `app/`：App Router 页面与 API 路由。
  - 首页：`/`（`app/page.tsx`，类型选择入口）
  - 填写页：`/[kind]`
  - 分享只读页：`/[kind]/s/[shareId]`
  - 趋势页：`/trends`
  - API：`app/api/*`
- `app/components/`：主业务组件（如 `My9V3App`、`v3/*`）。
- `components/`：跨页面复用组件（`layout/`、`share/`、`subject/`、`ui/`）。
- `lib/`：领域逻辑与工具（Bangumi 搜索、分享存储、`subject-kind` 等）。
- `tests/`：Playwright E2E 用例（当前为 `*.spec.ts`）。
- `scripts/playwright-webserver.cjs`：E2E 专用构建与 3001 服务脚本。
- `screenshot/`：验收截图产物。

## 构建、开发与测试命令
- `npm install`：安装依赖（建议 Node 18+）。
- `npm run dev`：本地开发（默认 `http://localhost:3000`）。
- `npm run build`：生产构建。
- `npm start`：启动生产构建产物。
- `npm run lint`：运行 ESLint。
- `npm run test:e2e`：运行 Playwright E2E。

说明：
- 仓库以 `npm` + `package-lock.json` 为准，避免切换包管理器引发锁文件噪音。

## Agent 端口与测试约定（强约束）
- `3000` 端口保留给开发者手动调试，自动化代理不得占用、停止或清理该端口进程。
- 自动化测试统一使用 `3001`。
- Playwright 通过 `scripts/playwright-webserver.cjs` 启动：
  - 使用独立构建目录 `.next-e2e`
  - 启动端口 `3001`
- 不要删除或覆盖开发者本地使用的 `.next`。

## 代码风格与实现约定
- 语言：TypeScript（`strict`），路径别名 `@/*`。
- 样式：Tailwind CSS；使用 `cn(...)` 合并类名。
- 组件与文件命名遵循现有风格（PascalCase 组件，`components/ui` 下文件名小写）。
- 优先做最小改动，保持当前交互与文案风格一致。

## 测试实践（当前状态）
- 本仓库已配置 Playwright。
- 新增/修改交互时，优先补充或更新 `tests/v3-interaction.spec.ts`。
- 涉及布局问题时，可补截图验证（保存到 `screenshot/`）。

## 环境变量与外部服务
- 在 `.env.local`（勿提交）中配置：
  - `BANGUMI_ACCESS_TOKEN`
  - `BANGUMI_USER_AGENT`
  - `UPSTASH_REDIS_REST_URL`、`UPSTASH_REDIS_REST_TOKEN`（如启用分享/趋势持久化）
- 分享图封面当前通过 `wsrv.nl` 在前端拉取并绘制；修改该链路时需评估跨域与流量成本影响。

## 提交与 PR 建议
- 提交信息简短、祈使/现在时，聚焦单一改动。
- PR 说明建议包含：改动范围、复现/验证步骤、必要截图、环境变量变更。
