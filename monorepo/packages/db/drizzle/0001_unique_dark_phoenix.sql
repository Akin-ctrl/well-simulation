CREATE TYPE "public"."status" AS ENUM('ACTIVE', 'DISABLED');--> statement-breakpoint
CREATE TYPE "public"."role" AS ENUM('USER', 'ADMIN', 'OPERATIONS');--> statement-breakpoint
CREATE TABLE "users" (
	"id" serial PRIMARY KEY NOT NULL,
	"email" varchar(256) NOT NULL,
	"first_name" varchar(256) NOT NULL,
	"last_name" varchar(256) DEFAULT '' NOT NULL,
	"user_name" varchar(256) NOT NULL,
	"phone_number" varchar(256),
	"bio" text,
	"image" varchar(256),
	"role" "role",
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"status" "status" DEFAULT 'ACTIVE',
	"deleted" boolean,
	"deleted_at" timestamp with time zone,
	"encrypted_password" varchar(256) NOT NULL,
	"last_signed_in" timestamp with time zone,
	"updated_at" timestamp with time zone DEFAULT now(),
	CONSTRAINT "users_email_unique" UNIQUE("email"),
	CONSTRAINT "users_user_name_unique" UNIQUE("user_name"),
	CONSTRAINT "users_phone_number_unique" UNIQUE("phone_number")
);
