import "./index.css";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router";
import { Toaster } from "sonner";
import { App } from "./App";
import { TooltipProvider } from "./components/ui/tooltip";
import { ApiError } from "./lib/api";

const qc = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 5_000,
      retry: (n, e) => !(e instanceof ApiError && e.status < 500) && n < 2,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={qc}>
      <TooltipProvider delayDuration={150}>
        <BrowserRouter>
          <App />
        </BrowserRouter>
        <Toaster position="bottom-right" theme="system" richColors closeButton
          toastOptions={{ className: "!rounded-xl !border-border !bg-surface !text-fg !shadow-xl" }} />
      </TooltipProvider>
    </QueryClientProvider>
  </StrictMode>,
);
