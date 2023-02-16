package kvdatabase

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Run(t *testing.T) {
	t.Run("Run ok", func(t *testing.T) {
		ftl, err := NewFileTransactionLogger("transaction.log")
		require.NoError(t, err)

		ftl.Run()

		_, err = os.Stat("transaction.log")
		require.NoError(t, err) // file exists
	})
}

func Test_ReadEvents(t *testing.T) {
	t.Run("ReadEvents ok", func(t *testing.T) {
		ftl, err := NewFileTransactionLogger("transaction.log")
		require.NoError(t, err)

		ftl.Run()
		ftl.WritePut("key-a", "value-a")
		events, errs := ftl.ReadEvents()
		select {
		case e, ok := <-events:
			require.True(t, ok)
			require.Equal(t, "key-a", e.Key)
		case err, ok := <-errs:
			require.False(t, ok)
			require.NoError(t, err)
		}

		err = os.Remove("transaction.log")
		require.NoError(t, err)
	})
}
