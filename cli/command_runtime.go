package cli

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/yaop-labs/queen"
)

type queenCommandFunc func(context.Context, *queen.Queen) error

func (app *App) runWithQueen(cmd *cobra.Command, fn queenCommandFunc) error {
	ctx := commandContext(cmd)
	return app.runWithQueenContext(ctx, nil, fn)
}

func (app *App) runWithQueenContext(ctx context.Context, opts []queen.Option, fn queenCommandFunc) error {
	restore := app.withQueenOptions(opts...)
	defer restore()

	q, err := app.setupQueen(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = q.Close() }()

	return fn(ctx, q)
}

func (app *App) withQueenOptions(opts ...queen.Option) func() {
	if len(opts) == 0 {
		return func() {}
	}

	previous := app.queenOpts
	app.queenOpts = append(append([]queen.Option{}, app.queenOpts...), opts...)
	return func() {
		app.queenOpts = previous
	}
}

func commandContext(cmd *cobra.Command) context.Context {
	if cmd == nil || cmd.Context() == nil {
		return context.Background()
	}
	return cmd.Context()
}
