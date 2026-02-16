package com.buuz135.simpleclaims.interactions;

import com.buuz135.simpleclaims.claim.ClaimManager;
import com.buuz135.simpleclaims.claim.party.PartyInfo;
import com.buuz135.simpleclaims.claim.party.PartyOverrides;
import com.hypixel.hytale.codec.builder.BuilderCodec;
import com.hypixel.hytale.component.Ref;
import com.hypixel.hytale.component.Store;
import com.hypixel.hytale.protocol.InteractionState;
import com.hypixel.hytale.protocol.InteractionType;
import com.hypixel.hytale.server.core.entity.InteractionContext;
import com.hypixel.hytale.server.core.entity.InteractionManager;
import com.hypixel.hytale.server.core.entity.entities.Player;
import com.hypixel.hytale.server.core.modules.interaction.interaction.CooldownHandler;
import com.hypixel.hytale.server.core.modules.interaction.interaction.config.none.ReplaceInteraction;
import com.hypixel.hytale.server.core.modules.interaction.interaction.config.server.RefillContainerInteraction;
import com.hypixel.hytale.server.core.universe.PlayerRef;
import com.hypixel.hytale.server.core.universe.world.storage.EntityStore;
import org.jetbrains.annotations.NotNull;

import java.util.function.Predicate;

public class ClaimReplaceInteraction extends ReplaceInteraction {

    public static final BuilderCodec<ClaimReplaceInteraction> CUSTOM_CODEC = BuilderCodec.builder(ClaimReplaceInteraction.class, ClaimReplaceInteraction::new, ReplaceInteraction.CODEC).build();


    @Override
    protected void tick0(boolean firstRun, float time, @NotNull InteractionType type, @NotNull InteractionContext context, @NotNull CooldownHandler cooldownHandler) {
        Ref<EntityStore> ref = context.getEntity();
        Store<EntityStore> store = ref.getStore();
        Player player = store.getComponent(ref, Player.getComponentType());
        PlayerRef playerRef = store.getComponent(ref, PlayerRef.getComponentType());
        Predicate<PartyInfo> defaultInteract = PartyInfo::isBlockBreakEnabled;
        var targetBlock = context.getTargetBlock();
        if (playerRef != null && ClaimManager.getInstance().isAllowedToInteract(playerRef.getUuid(), player.getWorld().getName(), targetBlock.x, targetBlock.z, defaultInteract, PartyOverrides.PARTY_PROTECTION_INTERACT)) {
            super.tick0(firstRun, time, type, context, cooldownHandler);
        } else {
            context.getState().state = InteractionState.Failed;
            InteractionManager manager = context.getInteractionManager();
            if (manager != null && context.getChain() != null) {
                manager.cancelChains(context.getChain());
            }
        }
    }

    @Override
    protected void simulateTick0(boolean firstRun, float time, @NotNull InteractionType type, @NotNull InteractionContext context, @NotNull CooldownHandler cooldownHandler) {
        Ref<EntityStore> ref = context.getEntity();
        Store<EntityStore> store = ref.getStore();
        Player player = store.getComponent(ref, Player.getComponentType());
        PlayerRef playerRef = store.getComponent(ref, PlayerRef.getComponentType());
        Predicate<PartyInfo> defaultInteract = PartyInfo::isBlockBreakEnabled;
        var targetBlock = context.getTargetBlock();
        if (playerRef != null && ClaimManager.getInstance().isAllowedToInteract(playerRef.getUuid(), player.getWorld().getName(), targetBlock.x, targetBlock.z, defaultInteract, PartyOverrides.PARTY_PROTECTION_INTERACT)) {
            super.simulateTick0(firstRun, time, type, context, cooldownHandler);
        } else {
            context.getState().state = InteractionState.Failed;
            InteractionManager manager = context.getInteractionManager();
            if (manager != null && context.getChain() != null) {
                manager.cancelChains(context.getChain());
            }
        }
    }
}
