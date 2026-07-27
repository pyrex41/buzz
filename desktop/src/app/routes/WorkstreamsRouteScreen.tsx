import { useAppNavigation } from "@/app/navigation/useAppNavigation";
import { useChannelsQuery } from "@/features/channels/hooks";
import { WorkstreamsScreen } from "@/features/workstreams/ui/WorkstreamsScreen";

type WorkstreamsRouteScreenProps = {
  selectedWorkstreamId: string | null;
};

export function WorkstreamsRouteScreen({
  selectedWorkstreamId,
}: WorkstreamsRouteScreenProps) {
  const { goWorkstream, goWorkstreams } = useAppNavigation();
  const channelsQuery = useChannelsQuery();
  const channels = channelsQuery.data ?? [];
  const memberChannels = channels.filter((channel) => channel.isMember);

  return (
    <WorkstreamsScreen
      channels={memberChannels}
      onCloseWorkstream={() => {
        void goWorkstreams();
      }}
      onSelectWorkstream={(workstreamId) => {
        void goWorkstream(workstreamId);
      }}
      selectedWorkstreamId={selectedWorkstreamId}
    />
  );
}
