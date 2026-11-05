use std::mem::size_of;

use crate::wire::{ActionSpec, Envelope, Frame, TargetInfo};

const REGISTRATION_OVERHEAD: usize = 256;

pub(crate) fn string_list_bytes(list: &[String]) -> usize {
    std::mem::size_of_val(list) + list.iter().map(String::len).sum::<usize>()
}

pub(crate) fn envelope_bytes(envelope: &Envelope) -> usize {
    size_of::<Envelope>()
        + envelope.target.len()
        + envelope.action.len()
        + string_list_bytes(&envelope.caps)
        + envelope.payload.len()
}

pub(crate) fn actions_bytes(actions: &[ActionSpec]) -> usize {
    actions
        .iter()
        .map(|action| size_of::<ActionSpec>() + action.name.len() + string_list_bytes(&action.caps))
        .sum()
}

pub(crate) fn frame_bytes(frame: &Frame) -> usize {
    let content = match frame {
        Frame::Hello { client, token } => client.len() + token.as_ref().map_or(0, String::len),
        Frame::Register {
            target,
            actions,
            groups,
        } => target.len() + actions_bytes(actions) + string_list_bytes(groups),
        Frame::Registered { target } => target.len(),
        Frame::Send(envelope) | Frame::Deliver(envelope) => envelope_bytes(envelope),
        Frame::Reply { payload, .. } | Frame::StreamItem { payload, .. } => payload.len(),
        Frame::Fail { message, .. } | Frame::Error { message, .. } => message.len(),
        Frame::Subscribe { target, event, .. } | Frame::Unsubscribe { target, event } => {
            target.len() + event.len()
        }
        Frame::Emit { event, payload } => event.len() + payload.len(),
        Frame::Event {
            target,
            event,
            payload,
        } => target.len() + event.len() + payload.len(),
        Frame::Targets { targets, .. } => targets
            .iter()
            .map(|info| {
                size_of::<TargetInfo>()
                    + info.name.len()
                    + actions_bytes(&info.actions)
                    + string_list_bytes(&info.groups)
            })
            .sum(),
        Frame::Welcome { .. }
        | Frame::Accepted { .. }
        | Frame::StreamEnd { .. }
        | Frame::ListTargets { .. }
        | Frame::Ping { .. }
        | Frame::Pong { .. } => 0,
    };
    size_of::<Frame>() + content
}

pub(crate) fn registration_bytes(target: &str, actions: &[ActionSpec], groups: &[String]) -> usize {
    REGISTRATION_OVERHEAD
        + target.len() * (1 + groups.len())
        + 2 * string_list_bytes(groups)
        + actions_bytes(actions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_strings_still_cost_their_containers() {
        let mut envelope = Envelope {
            id: 1,
            target: "t".into(),
            action: "a".into(),
            caps: Vec::new(),
            ttl: 0,
            retry: Default::default(),
            mode: Default::default(),
            payload: Vec::new(),
        };
        let base = envelope_bytes(&envelope);
        envelope.caps = vec![String::new(); 1000];
        assert!(envelope_bytes(&envelope) >= base + 1000 * size_of::<String>());

        let bare = registration_bytes("t", &[ActionSpec::new("a")], &[]);
        let capped = registration_bytes("t", &[ActionSpec::with_caps("a", vec!["a"; 1000])], &[]);
        assert!(capped >= bare + 1000 * (size_of::<String>() + 1));
    }
}
