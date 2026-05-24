package cli

import (
	"fmt"
	"os/user"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

func (app *App) gapIgnoreCmd() *cobra.Command {
	var reason string

	cmd := &cobra.Command{
		Use:   "ignore VERSION",
		Short: "Ignore a specific gap",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			version := args[0]

			qi, err := queen.LoadQueenIgnore()
			if err != nil {
				return fmt.Errorf("failed to load .queenignore: %w", err)
			}

			if qi.IsIgnored(version) {
				fmt.Printf("WARNING: Version %s is already ignored\n", version)
				if existingReason := qi.GetReason(version); existingReason != "" {
					fmt.Printf("   Existing reason: %s\n", existingReason)
				}
				return nil
			}

			ignoredBy := "unknown"
			if currentUser, userErr := user.Current(); userErr == nil {
				ignoredBy = currentUser.Username
			}

			if err := qi.AddIgnore(version, reason, ignoredBy); err != nil {
				return fmt.Errorf("failed to save .queenignore: %w", err)
			}

			fmt.Printf("Added version %s to .queenignore\n", version)
			if reason != "" {
				fmt.Printf("  Reason: %s\n", reason)
			}
			fmt.Println()
			fmt.Println("This gap will now be ignored by 'queen gap detect' and 'queen doctor'")

			return nil
		},
	}

	cmd.Flags().StringVar(&reason, "reason", "", "Reason for ignoring this gap")

	return cmd
}

func (app *App) gapListIgnoredCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-ignored",
		Short: "List all ignored gaps",
		RunE: func(cmd *cobra.Command, args []string) error {
			qi, err := queen.LoadQueenIgnore()
			if err != nil {
				return fmt.Errorf("failed to load .queenignore: %w", err)
			}

			ignored := qi.ListIgnored()
			if len(ignored) == 0 {
				fmt.Println("No ignored gaps")
				return nil
			}

			fmt.Printf("Ignored gaps (%d):\n\n", len(ignored))
			for _, gap := range ignored {
				fmt.Printf("  %s", gap.Version)
				if gap.Reason != "" {
					fmt.Printf(" - %s", gap.Reason)
				}
				fmt.Println()
			}

			return nil
		},
	}

	return cmd
}

func (app *App) gapUnignoreCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unignore VERSION",
		Short: "Remove a gap from ignore list",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			version := args[0]

			qi, err := queen.LoadQueenIgnore()
			if err != nil {
				return fmt.Errorf("failed to load .queenignore: %w", err)
			}

			if !qi.IsIgnored(version) {
				fmt.Printf("WARNING: Version %s is not in .queenignore\n", version)
				return nil
			}

			if err := qi.RemoveIgnore(version); err != nil {
				return fmt.Errorf("failed to save .queenignore: %w", err)
			}

			fmt.Printf("Removed version %s from .queenignore\n", version)
			fmt.Println()
			fmt.Println("This gap will now be detected by 'queen gap detect' and 'queen doctor'")

			return nil
		},
	}

	return cmd
}
