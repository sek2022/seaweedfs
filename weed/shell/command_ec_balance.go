package shell

import (
	"flag"
	"fmt"
	"io"
)

func init() {
	Commands = append(Commands, &commandEcBalance{})
}

type commandEcBalance struct {
}

func (c *commandEcBalance) Name() string {
	return "ec.balance"
}

// TODO: Update help string and move to command_ec_common.go once shard replica placement logic is enabled.
func (c *commandEcBalance) Help() string {
	return `balance all ec shards among all racks and volume servers

	ec.balance [-c EACH_COLLECTION|<collection_name>] [-force] [-dataCenter <data_center>] [-shardReplicaPlacement <replica_placement>] [-maxMoveShards <number>]

	Algorithm:
	` + ecBalanceAlgorithmDescription
}

func (c *commandEcBalance) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcBalance) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("collection", "EACH_COLLECTION", "collection name, or \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	shardReplicaPlacement := balanceCommand.String("shardReplicaPlacement", "", "replica placement for EC shards, or master default if empty")
	maxParallelization := balanceCommand.Int("maxParallelization", 10, "run up to X tasks in parallel, whenever possible")

	applyBalancing := balanceCommand.Bool("force", false, "apply the balancing plan")
	maxMoveShards := balanceCommand.Int("maxMoveShards", 0, "maximum number of shards to move in one operation, 0 means no limit")

	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *applyBalancing, "-force")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	var collections []string
	if *collection == "EACH_COLLECTION" {
		collections, err = ListCollectionNames(commandEnv, false, true)
		if err != nil {
			return err
		}
	} else {
		collections = append(collections, *collection)
	}
	fmt.Printf("balanceEcVolumes collections %+v, maxMoveShards %d\n", len(collections), *maxMoveShards)

	rp, err := parseReplicaPlacementArg(commandEnv, *shardReplicaPlacement)
	if err != nil {
		return err
	}

	return EcBalance(commandEnv, collections, *dc, rp, *applyBalancing, *maxMoveShards, *maxParallelization)
}
