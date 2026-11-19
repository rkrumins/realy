import { Compass } from "lucide-react";
import { Link } from "react-router";
import { EmptyState } from "@/components/common";
import { Button } from "@/components/ui/button";

export default function NotFound() {
  return (
    <EmptyState icon={<Compass />} title="Page not found" className="mt-10"
      action={<Button asChild variant="secondary"><Link to="/">Back to overview</Link></Button>}>
      The page you were looking for does not exist in this console.
    </EmptyState>
  );
}
