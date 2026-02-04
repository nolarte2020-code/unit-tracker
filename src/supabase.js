import { createClient } from "@supabase/supabase-js";

const supabaseUrl = "https://akrwvettjwszpjcanlrw.supabase.co";
const supabaseAnonKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFrcnd2ZXR0andzenBqY2FubHJ3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg3MjQ3ODQsImV4cCI6MjA4NDMwMDc4NH0.MV-e4vidfXyR0dxW5gca356Cck7-F0y2n17nYgHbVYc";

export const supabase = createClient(supabaseUrl, supabaseAnonKey);
